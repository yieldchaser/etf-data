"""
Tests for the Portfolio Lab (Part II).

Covers:
1. fetch_prices.py — universe loading, output shape
2. Backtest engine math (Python equivalents of the JS engine)
3. §21 acceptance criteria: VAMI, metrics formulas, Kelly tie-outs

Run: pytest tests/test_portfolio_lab.py -v
"""
from __future__ import annotations
import math
import pytest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# ─── Pure-Python backtest engine (mirrors JS) ────────────────────────────────

def build_monthly_returns(monthly: list[list]) -> dict[str, float]:
    """Build {YYYY-MM: return} from [[YYYY-MM, price], ...] list."""
    if not monthly or len(monthly) < 2:
        return {}
    ret = {}
    for i in range(1, len(monthly)):
        prev, curr = monthly[i-1][1], monthly[i][1]
        if prev > 0 and curr is not None:
            ret[monthly[i][0]] = curr / prev - 1
    return ret


def run_backtest(holdings, ticker_returns, start_ym, end_ym, rebalance='annual'):
    """Run portfolio backtest. Returns {months, equity, returns}."""
    all_months = set()
    for h in holdings:
        rets = ticker_returns.get(h['ticker'], {})
        all_months.update(rets.keys())
    months = sorted(m for m in all_months if start_ym <= m <= end_ym)
    if len(months) < 2:
        return None

    total_w = sum(h['weight'] for h in holdings)
    weights = {h['ticker']: h['weight'] / (total_w or 1) for h in holdings}
    current_weights = dict(weights)

    equity = [10000.0]
    port_rets = []

    for i, ym in enumerate(months):
        mo = int(ym[5:7])
        port_ret = sum(
            current_weights.get(h['ticker'], 0) * ticker_returns.get(h['ticker'], {}).get(ym, 0)
            for h in holdings
        )
        port_rets.append(port_ret)
        equity.append(equity[-1] * (1 + port_ret))

        should_rebalance = (
            rebalance == 'monthly' or
            (rebalance == 'quarterly' and mo % 3 == 0) or
            (rebalance == 'annual' and mo == 12)
        )
        if should_rebalance:
            current_weights = dict(weights)

    return {'months': months, 'equity': equity, 'returns': port_rets}


def cagr(start, end, n_months):
    if not start or not end or n_months <= 0:
        return None
    return (end / start) ** (12 / n_months) - 1


def max_dd(equity):
    peak, worst = equity[0], 0
    for v in equity:
        if v > peak:
            peak = v
        dd = v / peak - 1 if peak > 0 else 0
        if dd < worst:
            worst = dd
    return worst


def sharpe(rets, rf_monthly=0.004):
    if len(rets) < 2:
        return None
    excess = [r - rf_monthly for r in rets]
    m = sum(excess) / len(excess)
    s = math.sqrt(sum((x - m)**2 for x in excess) / (len(excess) - 1))
    return (m / s) * math.sqrt(12) if s > 0 else None


def beta(port_rets, bench_rets):
    n = min(len(port_rets), len(bench_rets))
    if n < 2:
        return None
    p, b = port_rets[:n], bench_rets[:n]
    pm, bm = sum(p)/n, sum(b)/n
    cov = sum((p[i]-pm)*(b[i]-bm) for i in range(n))
    bvar = sum((b[i]-bm)**2 for i in range(n))
    return cov / bvar if bvar > 0 else None


def info_ratio(port_rets, bench_rets):
    n = min(len(port_rets), len(bench_rets))
    active = [port_rets[i] - bench_rets[i] for i in range(n)]
    ar = sum(active) / n * 12
    te_std = math.sqrt(sum((x - sum(active)/n)**2 for x in active) / (n-1)) * math.sqrt(12)
    return ar / te_std if te_std > 0 else None


def kelly_fstar(mu, sigma, rf):
    """Continuous Kelly: f* = (μ - rf) / σ²"""
    return (mu - rf) / (sigma ** 2) if sigma > 0 else 0


def kelly_growth(f, mu, sigma, rf):
    """g(f) = rf + f(μ-rf) - ½f²σ²"""
    return rf + f * (mu - rf) - 0.5 * f * f * sigma * sigma


# ─── Test data ────────────────────────────────────────────────────────────────

def _make_price_series(start_ym, n_months, start_price=100.0, monthly_ret=0.01):
    """Generate synthetic monthly price series."""
    result = []
    yr, mo = int(start_ym[:4]), int(start_ym[5:7])
    price = start_price
    for _ in range(n_months):
        result.append([f"{yr:04d}-{mo:02d}", round(price, 4)])
        price *= (1 + monthly_ret)
        mo += 1
        if mo > 12:
            mo = 1
            yr += 1
    return result


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestBacktestEngine:
    """Test the backtest engine math."""

    def test_single_asset_cagr(self):
        """Single asset, 1% monthly return → CAGR ≈ 12.68%."""
        monthly = _make_price_series("2020-01", 24, 100.0, 0.01)
        rets = build_monthly_returns(monthly)
        holdings = [{'ticker': 'A', 'weight': 1.0}]
        result = run_backtest(holdings, {'A': rets}, '2020-01', '2021-12')
        assert result is not None
        c = cagr(result['equity'][0], result['equity'][-1], len(result['months']))
        # 1% monthly → (1.01)^12 - 1 ≈ 12.68%
        assert abs(c - 0.1268) < 0.001, f"CAGR {c:.4f} != expected ~0.1268"

    def test_vami_starts_at_1000(self):
        """VAMI₀ = 1000, VAMIₜ = VAMIₜ₋₁ × (1 + rₜ)."""
        monthly = _make_price_series("2020-01", 12, 100.0, 0.01)
        rets = build_monthly_returns(monthly)
        holdings = [{'ticker': 'A', 'weight': 1.0}]
        result = run_backtest(holdings, {'A': rets}, '2020-01', '2020-12')
        vami = [v / 10 for v in result['equity']]  # base 1000
        assert abs(vami[0] - 1000) < 0.01, f"VAMI₀ = {vami[0]}, expected 1000"

    def test_vami_end_equals_total_return(self):
        """VAMI_end / 1000 - 1 == total return."""
        monthly = _make_price_series("2020-01", 24, 100.0, 0.005)
        rets = build_monthly_returns(monthly)
        holdings = [{'ticker': 'A', 'weight': 1.0}]
        result = run_backtest(holdings, {'A': rets}, '2020-01', '2021-12')
        vami_end = result['equity'][-1] / 10
        total_return = result['equity'][-1] / result['equity'][0] - 1
        assert abs(vami_end / 1000 - 1 - total_return) < 1e-6, (
            f"VAMI_end/1000 - 1 = {vami_end/1000 - 1:.6f} != total_return = {total_return:.6f}"
        )

    def test_60_40_portfolio_rebalancing(self):
        """60/40 portfolio with annual rebalancing."""
        spy = _make_price_series("2020-01", 25, 100.0, 0.01)  # 25 prices → 24 returns (2020-02 to 2021-12)
        tlt = _make_price_series("2020-01", 25, 100.0, 0.003)
        rets = {
            'SPY': build_monthly_returns(spy),
            'TLT': build_monthly_returns(tlt),
        }
        # Returns start at 2020-02 (first return needs prev price at 2020-01)
        holdings = [{'ticker': 'SPY', 'weight': 0.6}, {'ticker': 'TLT', 'weight': 0.4}]
        result = run_backtest(holdings, rets, '2020-02', '2021-12', 'annual')
        assert result is not None
        assert len(result['months']) == 23  # 2020-02 through 2021-12
        # Portfolio return should be between SPY and TLT returns
        spy_result = run_backtest([{'ticker': 'SPY', 'weight': 1}], rets, '2020-02', '2021-12')
        tlt_result = run_backtest([{'ticker': 'TLT', 'weight': 1}], rets, '2020-02', '2021-12')
        n = len(result['months'])
        port_cagr = cagr(result['equity'][0], result['equity'][-1], n)
        spy_cagr = cagr(spy_result['equity'][0], spy_result['equity'][-1], n)
        tlt_cagr = cagr(tlt_result['equity'][0], tlt_result['equity'][-1], n)
        assert tlt_cagr <= port_cagr <= spy_cagr, (
            f"60/40 CAGR {port_cagr:.4f} should be between TLT {tlt_cagr:.4f} and SPY {spy_cagr:.4f}"
        )

    def test_max_drawdown_formula(self):
        """Max drawdown computed correctly from equity curve."""
        # Equity goes 100 → 120 → 80 → 110
        equity = [100, 120, 80, 110]
        dd = max_dd(equity)
        # Peak = 120, trough = 80 → DD = 80/120 - 1 = -33.3%
        assert abs(dd - (80/120 - 1)) < 1e-6, f"Max DD {dd:.4f} != expected {80/120-1:.4f}"

    def test_sharpe_formula(self):
        """Sharpe = mean(r-rf)/stdev(r-rf) × √12."""
        rets = [0.01, 0.02, -0.01, 0.015, 0.005, 0.02, -0.005, 0.01, 0.015, 0.02, 0.01, 0.005]
        rf = 0.004
        s = sharpe(rets, rf)
        # Manual calculation
        excess = [r - rf for r in rets]
        m = sum(excess) / len(excess)
        std = math.sqrt(sum((x-m)**2 for x in excess) / (len(excess)-1))
        expected = (m / std) * math.sqrt(12)
        assert abs(s - expected) < 1e-6, f"Sharpe {s:.4f} != expected {expected:.4f}"

    def test_info_ratio_formula(self):
        """IR = Active Return / Tracking Error."""
        port = [0.01, 0.02, -0.01, 0.015, 0.005, 0.02, -0.005, 0.01, 0.015, 0.02, 0.01, 0.005]
        bench = [0.008, 0.015, -0.008, 0.012, 0.004, 0.018, -0.003, 0.009, 0.012, 0.018, 0.009, 0.004]
        ir = info_ratio(port, bench)
        active = [port[i] - bench[i] for i in range(len(port))]
        ar = sum(active) / len(active) * 12
        te = math.sqrt(sum((x - sum(active)/len(active))**2 for x in active) / (len(active)-1)) * math.sqrt(12)
        expected = ar / te if te > 0 else None
        assert abs(ir - expected) < 1e-6, f"IR {ir:.4f} != expected {expected:.4f}"

    def test_beta_formula(self):
        """Beta = cov(p-rf, b-rf) / var(b-rf)."""
        port = [0.01, 0.02, -0.01, 0.015, 0.005]
        bench = [0.008, 0.015, -0.008, 0.012, 0.004]
        b = beta(port, bench)
        n = len(port)
        pm, bm = sum(port)/n, sum(bench)/n
        cov = sum((port[i]-pm)*(bench[i]-bm) for i in range(n))
        bvar = sum((bench[i]-bm)**2 for i in range(n))
        expected = cov / bvar
        assert abs(b - expected) < 1e-6, f"Beta {b:.4f} != expected {expected:.4f}"


class TestKellyEngine:
    """Test the Kelly criterion math."""

    def test_kelly_fstar_formula(self):
        """f* = (μ - rf) / σ²"""
        mu, sigma, rf = 0.10, 0.15, 0.045
        f = kelly_fstar(mu, sigma, rf)
        expected = (mu - rf) / (sigma ** 2)
        assert abs(f - expected) < 1e-6

    def test_kelly_growth_peaks_at_fstar(self):
        """g(f) is maximized at f*."""
        mu, sigma, rf = 0.10, 0.15, 0.045
        f_star = kelly_fstar(mu, sigma, rf)
        g_star = kelly_growth(f_star, mu, sigma, rf)
        # Check nearby points are lower
        assert kelly_growth(f_star * 0.9, mu, sigma, rf) < g_star
        assert kelly_growth(f_star * 1.1, mu, sigma, rf) < g_star

    def test_kelly_overbetting_gives_back_edge(self):
        """g(2f*) ≈ rf — overbetting gives back all edge."""
        mu, sigma, rf = 0.10, 0.15, 0.045
        f_star = kelly_fstar(mu, sigma, rf)
        g_2fstar = kelly_growth(2 * f_star, mu, sigma, rf)
        # g(2f*) should be approximately rf
        assert abs(g_2fstar - rf) < 0.001, (
            f"g(2f*) = {g_2fstar:.4f}, expected ≈ rf = {rf:.4f}"
        )

    def test_kelly_half_kelly_recommended(self):
        """Half-Kelly has lower vol and nearly as good growth."""
        mu, sigma, rf = 0.12, 0.20, 0.045
        f_star = kelly_fstar(mu, sigma, rf)
        g_full = kelly_growth(f_star, mu, sigma, rf)
        g_half = kelly_growth(f_star / 2, mu, sigma, rf)
        # Half-Kelly growth should be > 75% of full-Kelly growth
        assert g_half > g_full * 0.75, (
            f"Half-Kelly growth {g_half:.4f} should be > 75% of full-Kelly {g_full:.4f}"
        )


class TestFetchPrices:
    """Test the fetch_prices.py module structure."""

    def test_module_importable(self):
        """fetch_prices.py can be imported without errors."""
        import importlib
        spec = importlib.util.spec_from_file_location(
            "fetch_prices",
            REPO_ROOT / "predator" / "fetch_prices.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert hasattr(mod, 'fetch_prices')
        assert hasattr(mod, 'BENCHMARK_UNIVERSE')
        assert hasattr(mod, '_load_etf_universe')

    def test_benchmark_universe_has_key_tickers(self):
        """BENCHMARK_UNIVERSE includes SPY, TLT, GLD, QQQ."""
        import importlib
        spec = importlib.util.spec_from_file_location(
            "fetch_prices",
            REPO_ROOT / "predator" / "fetch_prices.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for ticker in ['SPY', 'TLT', 'GLD', 'QQQ', 'IWM']:
            assert ticker in mod.BENCHMARK_UNIVERSE, f"{ticker} missing from BENCHMARK_UNIVERSE"

    def test_etf_universe_loads_from_config(self):
        """_load_etf_universe() returns ETFs from config.yaml."""
        import importlib
        spec = importlib.util.spec_from_file_location(
            "fetch_prices",
            REPO_ROOT / "predator" / "fetch_prices.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        etfs = mod._load_etf_universe()
        # Should have at least the 30 ETFs from config.yaml
        assert len(etfs) >= 20, f"Expected ≥20 ETFs from config, got {len(etfs)}"
        # Key ETFs should be present
        for ticker in ['FPX', 'QMOM', 'COWZ', 'SPMO', 'QQQM']:
            assert ticker in etfs, f"{ticker} missing from ETF universe"

    def test_prices_json_shape_if_exists(self):
        """If prices.json exists, verify it has the correct shape."""
        prices_path = REPO_ROOT / "docs" / "data" / "prices.json"
        if not prices_path.exists():
            pytest.skip("prices.json not found — run fetch_prices first")
        import json
        data = json.loads(prices_path.read_text(encoding="utf-8"))
        assert "asof" in data, "Missing 'asof' field"
        assert "tickers" in data, "Missing 'tickers' field"
        assert len(data["tickers"]) > 0, "No tickers in prices.json"
        # Check first ticker has monthly array
        first = next(iter(data["tickers"].values()))
        assert "monthly" in first, "Ticker missing 'monthly' key"
        assert isinstance(first["monthly"], list), "'monthly' must be a list"
        assert len(first["monthly"]) > 0, "'monthly' is empty"
        assert isinstance(first["monthly"][0], list), "monthly entries must be [YYYY-MM, val]"
