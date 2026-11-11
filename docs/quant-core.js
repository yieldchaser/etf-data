/**
 * QuantCore - Mathematical and Taxonomy Engine for Leveraged Fund Flows
 *
 * Implements high-precision numerical algorithms, time-versioned taxonomy,
 * and microstructure estimators across browser and Node.js runtimes.
 *
 * Architecture Compliance:
 * - Pure algebraic and numerical routines with zero external runtime dependencies.
 * - Time-versioned leverage schedules and signed market beta attribution.
 * - Reconstructs Total Return series from authoritatively reported yields and NAVs.
 * - Zero emojis, zero banned terminology, strictly factual financial copy.
 */

(function (root, factory) {
  if (typeof define === 'function' && define.amd) {
    define([], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.QuantCore = factory();
  }
}(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // ── Canonical Underlying Identification & Taxonomy ──────────────────────

  const TICKER_TAXONOMY_MAP = {
    // 1. Broad Market Equity Index
    MIDU: ['SP400', 'S&P MidCap 400'],
    QLD: ['NDX', 'NASDAQ-100'],
    QQQM: ['NDX', 'NASDAQ-100'],
    SPHQ: ['SPX_QUALITY', 'S&P 500 Quality'],
    SPMO: ['SPX_MOMENTUM', 'S&P 500 Momentum'],
    SPXL: ['SPX', 'S&P 500'],
    SPXS: ['SPX', 'S&P 500'],
    SQQQ: ['NDX', 'NASDAQ-100'],
    SSO: ['SPX', 'S&P 500'],
    TNA: ['RUT', 'Russell 2000'],
    TQQQ: ['NDX', 'NASDAQ-100'],
    TZA: ['RUT', 'Russell 2000'],
    UDOW: ['DJIA', 'Dow Jones Industrial Average'],
    UPRO: ['SPX', 'S&P 500'],
    URTY: ['RUT', 'Russell 2000'],

    // 2. Technology, Semiconductor & Thematic
    BIB: ['BIOTECH_NDX', 'Nasdaq Biotechnology'],
    CWEB: ['CHINA_INET', 'China Internet'],
    DFEN: ['AERO_DEF', 'Aerospace & Defense'],
    HIBL: ['SPX_HIGHBETA', 'S&P 500 High Beta'],
    LABU: ['BIOTECH_SP', 'S&P Biotechnology'],
    MAGS: ['MAG7', 'Magnificent Seven'],
    MAGX: ['MAG7', 'Magnificent Seven'],
    MSOX: ['CANNABIS', 'U.S. Cannabis'],
    NAIL: ['HOMEBUILDERS', 'Homebuilders'],
    QQQU: ['MAG7', 'Magnificent Seven'],
    ROM: ['US_TECH', 'U.S. Technology'],
    SMH: ['SEMIS', 'Semiconductors'],
    SMHX: ['SEMIS', 'Semiconductors'],
    SOXL: ['SEMIS', 'Semiconductors'],
    SOXS: ['SEMIS', 'Semiconductors'],
    TARK: ['ARKK_INNOV', 'ARK Innovation Strategy'],
    TECL: ['US_TECH', 'U.S. Technology'],
    USD: ['SEMIS', 'Semiconductors'],
    WEBL: ['DJ_INTERNET', 'Dow Jones Internet'],
    XBI: ['BIOTECH_SP', 'S&P Biotechnology'],

    // 3. Sector Specific Leveraged
    CURE: ['HEALTHCARE', 'Health Care Select'],
    DIG: ['OIL_GAS', 'Oil & Gas'],
    DPST: ['US_REG_BANKS', 'Regional Banking'],
    DRN: ['US_REIT', 'U.S. Real Estate'],
    ERX: ['ENERGY_SELECT', 'Energy Select'],
    FAS: ['US_FIN', 'Financials Select'],
    PILL: ['PHARMA', 'Pharmaceuticals'],
    RETL: ['RETAIL', 'Retail'],
    URE: ['US_REIT', 'U.S. Real Estate'],
    UYG: ['US_FIN', 'Financials Select'],
    WANT: ['CONS_DISC', 'Consumer Discretionary'],

    // 4. Single-Stock Leveraged - AI, Semis & High-Beta Tech
    AAOX: ['AAOI', 'Applied Optoelectronics'],
    AMDL: ['AMD', 'Advanced Micro Devices'],
    APLX: ['APLD', 'Applied Digital'],
    APPX: ['APP', 'AppLovin'],
    ARMG: ['ARM', 'ARM Holdings'],
    ASMG: ['ASML', 'ASML Holding'],
    AVGX: ['AVGO', 'Broadcom'],
    AXTX: ['AXTI', 'AXT Inc'],
    COHX: ['COHR', 'Coherent Corp'],
    CRDU: ['CRDO', 'Credo Technology'],
    CRML: ['CRM', 'Salesforce'],
    CRWL: ['CRWD', 'CrowdStrike'],
    CSEX: ['CLS', 'Celestica'],
    CWVX: ['CRWV', 'CoreWeave'],
    DLLL: ['DELL', 'Dell Technologies'],
    GLWG: ['GLW', 'Corning'],
    INTW: ['INTC', 'Intel'],
    LABX: ['ALAB', 'Astera Labs'],
    LITX: ['LITE', 'Lumentum Holdings'],
    LRCU: ['LRCX', 'Lam Research'],
    MULL: ['MU', 'Micron Technology'],
    MUU: ['MU', 'Micron Technology'],
    MVLL: ['MRVL', 'Marvell Technology'],
    NEBX: ['NBIS', 'Nebius Group'],
    NOWL: ['NOW', 'ServiceNow'],
    NVD: ['NVDA', 'NVIDIA'],
    NVDL: ['NVDA', 'NVIDIA'],
    NVDS: ['NVDA', 'NVIDIA'],
    NVDU: ['NVDA', 'NVIDIA'],
    NVDX: ['NVDA', 'NVIDIA'],
    NVTX: ['NVTS', 'Navitas Semiconductor'],
    ORCX: ['ORCL', 'Oracle'],
    PANG: ['PANW', 'Palo Alto Networks'],
    PLTD: ['PLTR', 'Palantir Technologies'],
    PLTU: ['PLTR', 'Palantir Technologies'],
    PTIR: ['PLTR', 'Palantir Technologies'],
    QBTX: ['QBTS', 'D-Wave Quantum'],
    QCML: ['QCOM', 'Qualcomm'],
    RGTU: ['RGTI', 'Rigetti Computing'],
    RKLX: ['RKLB', 'Rocket Lab'],
    SMCX: ['SMCI', 'Super Micro Computer'],
    SNOU: ['SNOW', 'Snowflake'],
    SNXX: ['SNDK', 'SanDisk'],
    STXX: ['STX', 'Seagate Technology'],
    TEMT: ['TEM', 'Tempus AI'],
    TSMU: ['TSM', 'Taiwan Semiconductor'],
    WDCX: ['WDC', 'Western Digital'],

    // 5. Single-Stock Leveraged - Mega-Cap Giants, Crypto & Consumer
    AAPU: ['AAPL', 'Apple'],
    AMZU: ['AMZN', 'Amazon'],
    ASTX: ['ASTS', 'AST SpaceMobile'],
    BABX: ['BABA', 'Alibaba'],
    BEX: ['BE', 'Bloom Energy'],
    CONI: ['COIN', 'Coinbase'],
    CONL: ['COIN', 'Coinbase'],
    COTG: ['COST', 'Costco Wholesale'],
    FBL: ['META', 'Meta Platforms'],
    GEVX: ['GEV', 'GE Vernova'],
    GGLL: ['GOOGL', 'Alphabet'],
    GOOX: ['GOOGL', 'Alphabet'],
    HOOG: ['HOOD', 'Robinhood Markets'],
    IREX: ['IREN', 'IREN'],
    LLYX: ['LLY', 'Eli Lilly'],
    LULG: ['LULU', 'Lululemon Athletica'],
    METU: ['META', 'Meta Platforms'],
    MSFL: ['MSFT', 'Microsoft'],
    MSFU: ['MSFT', 'Microsoft'],
    MSTU: ['MSTR', 'MicroStrategy'],
    MSTX: ['MSTR', 'MicroStrategy'],
    MSTZ: ['MSTR', 'MicroStrategy'],
    NFXL: ['NFLX', 'Netflix'],
    SHPU: ['SHOP', 'Shopify'],
    SMST: ['MSTR', 'MicroStrategy'],
    SMU: ['SMR', 'NuScale Power'],
    SOFX: ['SOFI', 'SoFi Technologies'],
    TSLL: ['TSLA', 'Tesla'],
    TSLQ: ['TSLA', 'Tesla'],
    TSLR: ['TSLA', 'Tesla'],
    TSLT: ['TSLA', 'Tesla'],
    TSLZ: ['TSLA', 'Tesla'],
    UBRL: ['UBER', 'Uber Technologies'],
    UPSX: ['UPST', 'Upstart Holdings'],
    WULX: ['WULF', 'TeraWulf'],

    // 6. Commodities, Energy & Volatility
    AGQ: ['SILVER', 'Silver'],
    GDXU: ['GOLD_MINERS', 'Gold Miners'],
    JNUG: ['JR_GOLD_MINERS', 'Junior Gold Miners'],
    NUGT: ['GOLD_MINERS', 'Gold Miners'],
    SCO: ['CRUDE_OIL', 'Crude Oil'],
    SVIX: ['VIX_ST_FUTURES', 'S&P 500 VIX Short-Term Futures'],
    SVXY: ['VIX_ST_FUTURES', 'S&P 500 VIX Short-Term Futures'],
    UCO: ['CRUDE_OIL', 'Crude Oil'],
    UGL: ['GOLD', 'Gold'],
    UVIX: ['VIX_ST_FUTURES', 'S&P 500 VIX Short-Term Futures'],
    UVXY: ['VIX_ST_FUTURES', 'S&P 500 VIX Short-Term Futures'],

    // 7. Fixed Income, Currencies & Crypto
    BITU: ['BTC', 'Bitcoin'],
    BITX: ['BTC', 'Bitcoin'],
    ETHU: ['ETH', 'Ether'],
    IBIT: ['BTC', 'Bitcoin'],
    TLT: ['UST_20Y', '20+ Year Treasury'],
    TMF: ['UST_20Y', '20+ Year Treasury'],
    TMV: ['UST_20Y', '20+ Year Treasury'],
    TYD: ['UST_7_10Y', '7-10 Year Treasury'],
    UBT: ['UST_20Y', '20+ Year Treasury'],
    YCL: ['JPY_USD', 'JPY/USD'],

    // 8. International / Country
    BRZU: ['BRAZIL', 'Brazil'],
    CHAU: ['CHINA_A', 'China A-Shares'],
    EDC: ['EMERGING_MKTS', 'MSCI Emerging Markets'],
    EURL: ['EUROPE', 'MSCI Europe'],
    INDL: ['INDIA', 'India'],
    KORU: ['SOUTH_KOREA', 'South Korea'],
    MEXX: ['MEXICO', 'Mexico'],
    YANG: ['CHINA_50', 'China 50'],
    YINN: ['CHINA_50', 'China 50'],

    // 10. Core Benchmarks & Anchors
    ARKK: ['ARKK_INNOV', 'ARK Innovation Strategy'],
    COWZ: ['CASH_COWS', 'Pacer US Cash Cows 100']
  };

  const UNDERLYING_CANONICAL_MAP = [
    { id: 'NVDA', tickerAliases: ['NVDL', 'NVDX', 'NVDU', 'NVDS', 'NVD', 'NVDA'], underlyingPatterns: [/nvidia/i, /\bnvda\b/i], label: 'NVIDIA' },
    { id: 'NVTS', tickerAliases: ['NVTX', 'NVTS'], underlyingPatterns: [/navitas/i, /\bnvts\b/i], label: 'Navitas Semiconductor' },
    { id: 'TSLA', tickerAliases: ['TSLL', 'TSLR', 'TSLT', 'TSLQ', 'TSLZ', 'TSLA'], underlyingPatterns: [/tesla/i, /\btsla\b/i], label: 'Tesla' },
    { id: 'NDX', tickerAliases: ['QQQ', 'QQQM', 'QLD', 'TQQQ', 'SQQQ', 'NDX'], underlyingPatterns: [/nasdaq[- ]*100/i, /\bndx\b/i], label: 'NASDAQ-100' },
    { id: 'SPX_QUALITY', tickerAliases: ['SPHQ'], underlyingPatterns: [/s&p\s*500\s*quality/i], label: 'S&P 500 Quality' },
    { id: 'SPX_MOMENTUM', tickerAliases: ['SPMO'], underlyingPatterns: [/s&p\s*500\s*momentum/i], label: 'S&P 500 Momentum' },
    { id: 'SPX_HIGHBETA', tickerAliases: ['HIBL'], underlyingPatterns: [/s&p\s*500\s*high\s*beta/i], label: 'S&P 500 High Beta' },
    { id: 'SPX', tickerAliases: ['SPY', 'SPXL', 'UPRO', 'SSO', 'SPXS', 'SPX'], underlyingPatterns: [/s&p\s*500(?!\s*(?:vix|quality|momentum|high\s*beta))/i, /\bspx\b/i], label: 'S&P 500' },
    { id: 'SP400', tickerAliases: ['MIDU'], underlyingPatterns: [/midcap\s*400/i, /s&p\s*midcap\s*400/i], label: 'S&P MidCap 400' },
    { id: 'DJIA', tickerAliases: ['UDOW', 'DIA', 'DJIA'], underlyingPatterns: [/dow\s*jones\s*(?:industrial|30)/i], label: 'Dow Jones Industrial Average' },
    { id: 'RUT', tickerAliases: ['TNA', 'TZA', 'URTY', 'IWM', 'RUT'], underlyingPatterns: [/russell\s*2000/i, /\brut\b/i], label: 'Russell 2000' },
    { id: 'UST_20Y', tickerAliases: ['TLT', 'TMF', 'UBT', 'TMV'], underlyingPatterns: [/20\+\s*year\s*treasury/i, /ice\s*u\.s\.\s*treasury\s*20\+/i], label: '20+ Year Treasury' },
    { id: 'UST_7_10Y', tickerAliases: ['TYD'], underlyingPatterns: [/7[- ]10\s*year\s*treasury/i], label: '7-10 Year Treasury' },
    { id: 'SEMIS', tickerAliases: ['SOXL', 'SOXS', 'SMH', 'SMHX', 'USD'], underlyingPatterns: [/semiconductor/i, /\bmvis\b.*semi/i], label: 'Semiconductors' },
    { id: 'VIX_ST_FUTURES', tickerAliases: ['UVIX', 'UVXY', 'SVIX', 'SVXY'], underlyingPatterns: [/vix/i], label: 'S&P 500 VIX Short-Term Futures' },
    { id: 'MSTR', tickerAliases: ['MSTR', 'MSTU', 'MSTX', 'MSTZ', 'SMST'], underlyingPatterns: [/microstrategy/i, /\bmstr\b/i], label: 'MicroStrategy' },
    { id: 'PLTR', tickerAliases: ['PLTR', 'PLTU', 'PLTD', 'PTIR'], underlyingPatterns: [/palantir/i, /\bpltr\b/i], label: 'Palantir Technologies' },
    { id: 'BTC', tickerAliases: ['BTC', 'BITU', 'BITX', 'IBIT'], underlyingPatterns: [/bitcoin/i, /\bbtc\b/i], label: 'Bitcoin' },
    { id: 'ETH', tickerAliases: ['ETH', 'ETHU'], underlyingPatterns: [/\bether\b/i, /\bethereum\b/i, /\beth\b/i], label: 'Ether' },
    { id: 'JPY_USD', tickerAliases: ['YCL'], underlyingPatterns: [/jpy\s*\/\s*usd/i, /japanese\s*yen/i], label: 'JPY/USD' },
    { id: 'CRUDE_OIL', tickerAliases: ['UCO', 'SCO'], underlyingPatterns: [/crude\s*oil/i], label: 'Crude Oil' },
    { id: 'CHINA_50', tickerAliases: ['YINN', 'YANG'], underlyingPatterns: [/china\s*50/i], label: 'China 50' },
    { id: 'CHINA_A', tickerAliases: ['CHAU'], underlyingPatterns: [/china\s*a[- ]*shares/i], label: 'China A-Shares' },
    { id: 'CHINA_INET', tickerAliases: ['CWEB'], underlyingPatterns: [/china\s*internet/i], label: 'China Internet' },
    { id: 'JR_GOLD_MINERS', tickerAliases: ['JNUG'], underlyingPatterns: [/junior\s*gold\s*miners/i], label: 'Junior Gold Miners' },
    { id: 'GOLD_MINERS', tickerAliases: ['GDXU', 'NUGT'], underlyingPatterns: [/gold\s*miners/i], label: 'Gold Miners' },
    { id: 'GOLD', tickerAliases: ['UGL'], underlyingPatterns: [/\bgold\b/i], label: 'Gold' },
    { id: 'SILVER', tickerAliases: ['AGQ'], underlyingPatterns: [/silver/i], label: 'Silver' },
    { id: 'ARKK_INNOV', tickerAliases: ['ARKK', 'TARK'], underlyingPatterns: [/disruptive\s*innovation/i, /ark\s*innovation/i, /\barkk\b/i], label: 'ARK Innovation Strategy' },
    { id: 'COIN', tickerAliases: ['COIN', 'CONI', 'CONL'], underlyingPatterns: [/coinbase/i, /\bcoin\b/i], label: 'Coinbase' },
    { id: 'MU', tickerAliases: ['MU', 'MULL', 'MUU'], underlyingPatterns: [/micron/i], label: 'Micron Technology' },
    { id: 'BIOTECH_NDX', tickerAliases: ['BIB'], underlyingPatterns: [/nasdaq\s*biotech/i], label: 'Nasdaq Biotechnology' },
    { id: 'BIOTECH_SP', tickerAliases: ['LABU', 'XBI'], underlyingPatterns: [/s&p\s*biotech/i], label: 'S&P Biotechnology' },
    { id: 'US_FIN', tickerAliases: ['FAS', 'UYG'], underlyingPatterns: [/financials/i], label: 'Financials Select' },
    { id: 'US_TECH', tickerAliases: ['TECL', 'ROM'], underlyingPatterns: [/technology\s*select/i, /u\.s\.\s*technology/i], label: 'U.S. Technology' },
    { id: 'US_REIT', tickerAliases: ['DRN', 'URE'], underlyingPatterns: [/reit/i, /real\s*estate/i], label: 'U.S. Real Estate' },
    { id: 'ENERGY_SELECT', tickerAliases: ['ERX'], underlyingPatterns: [/energy\s*select/i], label: 'Energy Select' },
    { id: 'OIL_GAS', tickerAliases: ['DIG'], underlyingPatterns: [/oil\s*&\s*gas/i], label: 'Oil & Gas' },
    { id: 'HEALTHCARE', tickerAliases: ['CURE'], underlyingPatterns: [/health\s*care/i], label: 'Health Care Select' },
    { id: 'CONS_DISC', tickerAliases: ['WANT'], underlyingPatterns: [/consumer\s*discretionary/i], label: 'Consumer Discretionary' },
    { id: 'RETAIL', tickerAliases: ['RETL'], underlyingPatterns: [/\bretail\b/i], label: 'Retail' },
    { id: 'AERO_DEF', tickerAliases: ['DFEN'], underlyingPatterns: [/aerospace/i], label: 'Aerospace & Defense' },
    { id: 'HOMEBUILDERS', tickerAliases: ['NAIL'], underlyingPatterns: [/homebuilders/i], label: 'Homebuilders' },
    { id: 'US_REG_BANKS', tickerAliases: ['DPST'], underlyingPatterns: [/regional\s*bank/i], label: 'Regional Banking' },
    { id: 'DJ_INTERNET', tickerAliases: ['WEBL'], underlyingPatterns: [/dow\s*jones\s*internet/i], label: 'Dow Jones Internet' },
    { id: 'MAG7', tickerAliases: ['MAGS', 'MAGX', 'QQQU'], underlyingPatterns: [/magnificent\s*seven/i], label: 'Magnificent Seven' },
    { id: 'AAPL', tickerAliases: ['AAPL', 'AAPU'], underlyingPatterns: [/apple/i, /\baapl\b/i], label: 'Apple' },
    { id: 'AMZN', tickerAliases: ['AMZN', 'AMZU'], underlyingPatterns: [/amazon/i, /\bamzn\b/i], label: 'Amazon' },
    { id: 'GOOGL', tickerAliases: ['GOOGL', 'GGLL', 'GOOX'], underlyingPatterns: [/alphabet/i, /\bgoogl\b/i], label: 'Alphabet' },
    { id: 'MSFT', tickerAliases: ['MSFT', 'MSFL', 'MSFU'], underlyingPatterns: [/microsoft/i, /\bmsft\b/i], label: 'Microsoft' },
    { id: 'META', tickerAliases: ['META', 'FBL', 'METU'], underlyingPatterns: [/meta\s*platforms/i], label: 'Meta Platforms' }
  ];

  const LEVERAGE_SCHEDULES = {
    UVXY: [
      { until: '2018-02-27', leverage: 2.0 },
      { since: '2018-02-28', leverage: 1.5 }
    ],
    SVXY: [
      { until: '2018-02-27', leverage: -1.0 },
      { since: '2018-02-28', leverage: -0.5 }
    ],
    SMST: [
      { until: '2024-10-28', leverage: -1.5 },
      { since: '2024-10-29', leverage: -2.0 }
    ]
  };

  function canonicalUnderlyingId(rawUnderlying, ticker) {
    const raw = String(rawUnderlying || '').trim();
    const tk = String(ticker || '').trim().toUpperCase();

    if (tk && Object.prototype.hasOwnProperty.call(TICKER_TAXONOMY_MAP, tk)) {
      return TICKER_TAXONOMY_MAP[tk][0];
    }

    if (tk) {
      for (let i = 0; i < UNDERLYING_CANONICAL_MAP.length; i++) {
        const aliases = UNDERLYING_CANONICAL_MAP[i].tickerAliases || [];
        if (aliases.includes(tk)) return UNDERLYING_CANONICAL_MAP[i].id;
      }
    }

    for (let i = 0; i < UNDERLYING_CANONICAL_MAP.length; i++) {
      const pats = UNDERLYING_CANONICAL_MAP[i].underlyingPatterns || [];
      for (let p = 0; p < pats.length; p++) {
        if (pats[p].test(raw)) {
          return UNDERLYING_CANONICAL_MAP[i].id;
        }
      }
    }

    const cleaned = raw.replace(/\s*\([^)]*\)\s*$/, '').trim().toUpperCase().replace(/[^A-Z0-9]+/g, '_');
    return cleaned || (tk || 'UNKNOWN');
  }

  function canonicalUnderlyingLabel(id, raw) {
    for (const entry of Object.values(TICKER_TAXONOMY_MAP)) {
      if (entry[0] === id) return entry[1];
    }
    const match = UNDERLYING_CANONICAL_MAP.find(e => e.id === id);
    if (match) return match.label;
    if (raw) {
      return String(raw).replace(/\s*\([^)]*\)\s*$/, '').trim();
    }
    return String(id || 'Unknown');
  }

  // ── Time-Versioned Leverage & Beta Attribution ──────────────────────────

  function leverageAtDate(ticker, dateStr, defaultLev, customSchedule) {
    const tk = String(ticker || '').trim().toUpperCase();
    const parsedDef = Number(defaultLev);
    const def = (defaultLev != null && defaultLev !== '' && Number.isFinite(parsedDef)) ? parsedDef : 1.0;
    const sched = (Array.isArray(customSchedule) && customSchedule.length)
      ? customSchedule
      : LEVERAGE_SCHEDULES[tk];

    if (sched && sched.length) {
      const dt = String(dateStr || '').trim().slice(0, 10);
      if (!dt) {
        return Number(sched[sched.length - 1].leverage);
      }
      for (let i = 0; i < sched.length; i++) {
        const rule = sched[i];
        const untilD = rule.until;
        const sinceD = rule.since;
        if (untilD && dt <= untilD && !sinceD) return Number(rule.leverage);
        if (sinceD && dt >= sinceD && (!untilD || dt <= untilD)) return Number(rule.leverage);
      }
      return Number(sched[sched.length - 1].leverage);
    }
    return def;
  }

  function marketBetaPolarity(ticker, leverageValue) {
    const tk = String(ticker || '').trim().toUpperCase();
    const parsedLev = Number(leverageValue);
    let lev;
    if (leverageValue != null && leverageValue !== '' && Number.isFinite(parsedLev)) {
      lev = parsedLev;
    } else if (LEVERAGE_SCHEDULES[tk]) {
      const sched = LEVERAGE_SCHEDULES[tk];
      lev = Number(sched[sched.length - 1].leverage);
    } else {
      lev = 1.0;
    }

    if (canonicalUnderlyingId('', tk) === 'VIX_ST_FUTURES') {
      return lev > 0 ? -1 : 1;
    }
    return lev >= 0 ? 1 : -1;
  }

  function classifyPairMatch(instA, instB, dateStr) {
    if (!instA || !instB) return null;
    const undA = canonicalUnderlyingId(instA.underlying, instA.ticker);
    const undB = canonicalUnderlyingId(instB.underlying, instB.ticker);
    if (!undA || undA !== undB) return null;

    const levA = leverageAtDate(instA.ticker, dateStr, instA.leverage_value);
    const levB = leverageAtDate(instB.ticker, dateStr, instB.leverage_value);

    // Must be opposite directions
    if (levA * levB >= 0) return null;

    if (Math.abs(Math.abs(levA) - Math.abs(levB)) < 1e-6) {
      return 'symmetric';
    }
    return 'asymmetric_hedge';
  }

  function areSymmetricPair(instA, instB, dateStr) {
    return classifyPairMatch(instA, instB, dateStr) === 'symmetric';
  }

  // ── Total Return Index Series Reconstruction ────────────────────────────

  function reconstructTotalReturnSeries(bars, flowRecords) {
    if (!Array.isArray(bars) || bars.length === 0) return [];
    const N = bars.length;

    // Check if all bars already carry valid Total Return Close (tc)
    const hasAllPrecomputedTC = bars.every(b => typeof b.tc === 'number' && Number.isFinite(b.tc) && b.tc > 0);
    if (hasAllPrecomputedTC) {
      return bars.map(b => b.tc);
    }

    // Build date-indexed map of flow records
    const flowMap = new Map();
    if (Array.isArray(flowRecords)) {
      for (let i = 0; i < flowRecords.length; i++) {
        const r = flowRecords[i];
        if (r && r.date) flowMap.set(r.date, r);
      }
    } else if (flowRecords && typeof flowRecords === 'object') {
      for (const k of Object.keys(flowRecords)) {
        flowMap.set(k, flowRecords[k]);
      }
    }

    // Reconstruct backwards from latest bar N-1 with A_TR[N-1] = 1.0
    const A = new Float64Array(N);
    A[N - 1] = 1.0;

    for (let t = N - 1; t >= 1; t--) {
      const bCurr = bars[t];
      const bPrev = bars[t - 1];
      const dCurr = bCurr.d || bCurr.date;
      const dPrev = bPrev.d || bPrev.date;

      const cCurr = Number(bCurr.c ?? bCurr.close);
      const cPrev = Number(bPrev.c ?? bPrev.close);
      const tcCurr = Number(bCurr.tc);
      const tcPrev = Number(bPrev.tc);

      let dYield = 0.0;

      if (
        Number.isFinite(tcCurr) && tcCurr > 0 && Number.isFinite(cCurr) && cCurr > 0 &&
        Number.isFinite(tcPrev) && tcPrev > 0 && Number.isFinite(cPrev) && cPrev > 0
      ) {
        // Respect precomputed TRI/raw ratio delta when adjacent bars already carry tc
        const aCurr = tcCurr / cCurr;
        const aPrev = tcPrev / cPrev;
        if (aCurr > 0 && aPrev > 0) {
          dYield = Math.log(aCurr / aPrev);
        }
      } else {
        const fCurr = flowMap.get(dCurr);
        const fPrev = flowMap.get(dPrev);
        if (
          fCurr && fPrev &&
          Number.isFinite(fCurr.performance) && Number.isFinite(fCurr.nav) && Number.isFinite(fPrev.nav)
        ) {
          const perf = fCurr.performance;
          const navCurr = fCurr.nav;
          const navPrev = fPrev.nav;

          if (perf > -1.0 && navCurr > 0 && navPrev > 0) {
            const rPerf = Math.log1p(perf);
            const rNav = Math.log(navCurr / navPrev);
            const rawYield = rPerf - rNav;

            // Gate against split jumps and extreme outliers while allowing large special distributions
            if (rawYield > 0.0 && rawYield <= 0.65) {
              dYield = rawYield;
            }
          }
        }
      }

      A[t - 1] = A[t] * Math.exp(-dYield);
    }

    const triSeries = new Array(N);
    for (let t = 0; t < N; t++) {
      const bar = bars[t];
      if (typeof bar.tc === 'number' && Number.isFinite(bar.tc) && bar.tc > 0) {
        triSeries[t] = bar.tc;
        continue;
      }
      const closeVal = Number(bar.c ?? bar.close);
      triSeries[t] = (Number.isFinite(closeVal) && closeVal > 0) ? (closeVal * A[t]) : null;
    }

    return triSeries;
  }

  // ── Numerical Algorithms & Microstructure Estimators ────────────────────

  function yangZhangVolatility(bars, windowSize = 21, annualized = false) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const n = Math.max(5, Math.trunc(windowSize || 21));
    const result = new Array(N).fill(null);
    if (N < n + 1) return result;

    const k = 0.34 / (1.34 + (n + 1) / (n - 1));
    const annualFactor = annualized ? Math.sqrt(252) : 1.0;

    for (let t = n; t < N; t++) {
      let sumO = 0;
      let sumC = 0;
      let sumRS = 0;
      const oArr = new Float64Array(n);
      const cArr = new Float64Array(n);

      let validWindow = true;
      for (let j = 0; j < n; j++) {
        const idx = t - n + 1 + j;
        const curr = bars[idx];
        const prev = bars[idx - 1];

        const o = Number(curr.o ?? curr.open);
        const h = Number(curr.h ?? curr.high);
        const l = Number(curr.l ?? curr.low);
        const c = Number(curr.c ?? curr.close);
        const cPrev = Number(prev.c ?? prev.close);

        if (
          !(o > 0 && h > 0 && l > 0 && c > 0 && cPrev > 0 &&
            Number.isFinite(o) && Number.isFinite(h) && Number.isFinite(l) &&
            Number.isFinite(c) && Number.isFinite(cPrev))
        ) {
          validWindow = false;
          break;
        }

        const logO = Math.log(o / cPrev);
        const logC = Math.log(c / o);
        const logU = Math.log(h / o);
        const logD = Math.log(l / o);

        const rs = logU * (logU - logC) + logD * (logD - logC);

        oArr[j] = logO;
        cArr[j] = logC;
        sumO += logO;
        sumC += logC;
        sumRS += rs;
      }

      if (!validWindow) continue;

      const meanO = sumO / n;
      const meanC = sumC / n;

      let varO = 0;
      let varC = 0;
      for (let j = 0; j < n; j++) {
        varO += (oArr[j] - meanO) ** 2;
        varC += (cArr[j] - meanC) ** 2;
      }
      varO /= (n - 1);
      varC /= (n - 1);

      const meanRS = sumRS / n;
      const yzVar = varO + k * varC + (1 - k) * meanRS;
      const yzVol = Math.sqrt(Math.max(1e-12, yzVar)) * annualFactor;
      result[t] = Number.isFinite(yzVol) ? yzVol : null;
    }

    return result;
  }

  function studentT4CDF(z) {
    if (!Number.isFinite(z)) return z < 0 ? 0.0 : 1.0;
    const z2 = z * z;
    return 0.5 + (z * (6 + z2)) / (2 * Math.pow(4 + z2, 1.5));
  }

  function studentT5CDF(t) {
    if (!Number.isFinite(t)) return t < 0 ? 0.0 : 1.0;
    const theta = Math.atan(t / Math.sqrt(5));
    const cos = Math.cos(theta);
    const sin = Math.sin(theta);
    return 0.5 + (1 / Math.PI) * (theta + sin * cos * (1 + (2 / 3) * cos * cos));
  }

  function normalQuantile(p) {
    if (p <= 0.0) return -Infinity;
    if (p >= 1.0) return Infinity;

    const a = [
      -3.969683028665376e+01,  2.209460984245205e+02,
      -2.759285104469687e+02,  1.383577518672690e+02,
      -3.066479806614716e+01,  2.506628277459239e+00
    ];
    const b = [
      -5.447609879822406e+01,  1.615858368580409e+02,
      -1.556989798598866e+02,  6.680131188771972e+01,
      -1.328068155288572e+01
    ];
    const c = [
      -7.784894002430293e-03, -3.223964580411365e-01,
      -2.400758277161838e+00, -2.549732539343734e+00,
       4.374664141464968e+00,  2.938163982698783e+00
    ];
    const d = [
       7.784695709041462e-03,  3.224671290700398e-01,
       2.445134137142996e+00,  3.754408661907416e+00
    ];

    const pLow = 0.02425;
    const pHigh = 1 - pLow;

    if (p < pLow) {
      const q = Math.sqrt(-2 * Math.log(p));
      return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
             ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
    }
    if (p <= pHigh) {
      const q = p - 0.5;
      const r = q * q;
      return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q /
             (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
    }
    const q = Math.sqrt(-2 * Math.log(1 - p));
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }

  function normalCDF(x) {
    if (!Number.isFinite(x)) return x < 0 ? 0.0 : 1.0;
    const a1 =  0.254829592;
    const a2 = -0.284496736;
    const a3 =  1.421413741;
    const a4 = -1.453152027;
    const a5 =  1.061405429;
    const p  =  0.3275911;

    const sign = x < 0 ? -1 : 1;
    const absX = Math.abs(x) / Math.SQRT2;
    const t = 1.0 / (1.0 + p * absX);
    const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX);
    return 0.5 * (1.0 + sign * y);
  }

  function rollingAbdiRanaldo21(bars, absLev = 1.0, windowSize = 21, ticker = null) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const w = Math.max(5, Math.trunc(windowSize || 21));
    const parsedLev = Math.abs(Number(absLev));
    const lev = Math.max(0.1, Number.isFinite(parsedLev) && parsedLev > 0 ? parsedLev : 1.0);
    const result = new Array(N).fill(null);
    if (N < w + 1) return result;

    const terms = new Float64Array(N);
    const valid = new Uint8Array(N);

    for (let t = 1; t < N; t++) {
      const prev = bars[t - 1];
      const curr = bars[t];

      const hp = Number(prev.h ?? prev.high);
      const lp = Number(prev.l ?? prev.low);
      const cp = Number(prev.c ?? prev.close);

      const hc = Number(curr.h ?? curr.high);
      const lc = Number(curr.l ?? curr.low);

      if (
        !(Number.isFinite(hp) && Number.isFinite(lp) && Number.isFinite(cp) &&
          Number.isFinite(hc) && Number.isFinite(lc) &&
          hp > 0 && lp > 0 && cp > 0 && hc > 0 && lc > 0 &&
          hp > lp && hc > lc)
      ) {
        continue;
      }

      const etaPrev = 0.5 * (Math.log(hp) + Math.log(lp));
      const etaCurr = 0.5 * (Math.log(hc) + Math.log(lc));
      const logCp = Math.log(cp);
      const levT = ticker
        ? Math.max(0.1, Math.abs(leverageAtDate(ticker, curr.d || curr.date, lev)))
        : lev;

      terms[t] = (4.0 * (logCp - etaPrev) * (logCp - etaCurr)) / (levT * levT);
      valid[t] = 1;
    }

    for (let t = w; t < N; t++) {
      let sum = 0.0;
      let count = 0;
      for (let j = t - w + 1; j <= t; j++) {
        if (valid[j]) {
          sum += terms[j];
          count++;
        }
      }
      if (count >= Math.floor(w * 0.6)) {
        const meanTerm = sum / count;
        const spread = Math.sqrt(Math.max(0.0, meanTerm));
        result[t] = Number.isFinite(spread) ? spread : null;
      }
    }

    return result;
  }

  function rollingCorwinSchultz21(bars, absLev = 1.0, windowSize = 21, ticker = null) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const w = Math.max(5, Math.trunc(windowSize || 21));
    const parsedLev = Math.abs(Number(absLev));
    const lev = Math.max(0.1, Number.isFinite(parsedLev) && parsedLev > 0 ? parsedLev : 1.0);
    const result = new Array(N).fill(null);
    if (N < w + 1) return result;

    const dailySpreads = new Float64Array(N);
    const valid = new Uint8Array(N);
    const denom = 3.0 - 2.0 * Math.SQRT2;

    for (let t = 1; t < N; t++) {
      const prev = bars[t - 1];
      const curr = bars[t];

      const hp = Number(prev.h ?? prev.high);
      const lp = Number(prev.l ?? prev.low);
      const hc = Number(curr.h ?? curr.high);
      const lc = Number(curr.l ?? curr.low);

      if (
        !(Number.isFinite(hp) && Number.isFinite(lp) && Number.isFinite(hc) && Number.isFinite(lc) &&
          hp > 0 && lp > 0 && hc > 0 && lc > 0 && hp > lp && hc > lc)
      ) {
        continue;
      }

      const beta = (Math.log(hp / lp) ** 2) + (Math.log(hc / lc) ** 2);
      const gamma = (Math.log(Math.max(hp, hc) / Math.min(lp, lc)) ** 2);

      const alpha = (Math.sqrt(2.0 * beta) - Math.sqrt(beta)) / denom - Math.sqrt(gamma / denom);
      if (!Number.isFinite(alpha)) continue;

      const levT = ticker
        ? Math.max(0.1, Math.abs(leverageAtDate(ticker, curr.d || curr.date, lev)))
        : lev;
      // Use tanh(alpha / 2) = (e^alpha - 1) / (e^alpha + 1) so large alpha cannot overflow to Infinity/Infinity = NaN
      const s = (1.0 / levT) * Math.max(0.0, 2.0 * Math.tanh(alpha / 2.0));

      dailySpreads[t] = s;
      valid[t] = 1;
    }

    for (let t = w; t < N; t++) {
      let sum = 0.0;
      let count = 0;
      for (let j = t - w + 1; j <= t; j++) {
        if (valid[j]) {
          sum += dailySpreads[j];
          count++;
        }
      }
      if (count >= Math.floor(w * 0.6)) {
        result[t] = sum / count;
      }
    }

    return result;
  }

  function holderPowerMean(values, weights, p = 3.0) {
    if (!Array.isArray(values) || values.length === 0) return 0.0;
    const N = values.length;
    const power = Number(p) || 3.0;

    let wSum = 0.0;
    const wArr = new Float64Array(N);
    for (let i = 0; i < N; i++) {
      const w = Array.isArray(weights) && weights[i] !== undefined ? Math.max(0.0, Number(weights[i]) || 0.0) : 1.0;
      wArr[i] = w;
      wSum += w;
    }
    if (wSum <= 0.0) return 0.0;

    let acc = 0.0;
    for (let i = 0; i < N; i++) {
      const v = Math.max(0.0, Number(values[i]) || 0.0);
      const wNorm = wArr[i] / wSum;
      acc += wNorm * Math.pow(v, power);
    }

    return Math.pow(Math.max(0.0, acc), 1.0 / power);
  }

  function benjaminiHochbergFDR(pValues, q = 0.05) {
    if (!Array.isArray(pValues) || pValues.length === 0) return [];
    const m = pValues.length;
    const thresholdQ = Math.max(0.001, Math.min(0.5, Number(q) || 0.05));

    const indexed = new Array(m);
    for (let i = 0; i < m; i++) {
      const pv = Number(pValues[i]);
      indexed[i] = { p: Number.isFinite(pv) ? Math.max(0.0, Math.min(1.0, pv)) : 1.0, origIdx: i };
    }

    indexed.sort((a, b) => a.p - b.p);

    let maxK = -1;
    for (let k = m - 1; k >= 0; k--) {
      const criticalVal = ((k + 1) / m) * thresholdQ;
      if (indexed[k].p <= criticalVal) {
        maxK = k;
        break;
      }
    }

    const isSignificant = new Array(m).fill(false);
    for (let k = 0; k <= maxK; k++) {
      isSignificant[indexed[k].origIdx] = true;
    }

    return isSignificant;
  }

  // ── Public UMD Module Exports ───────────────────────────────────────────

  return Object.freeze({
    canonicalUnderlyingId,
    canonicalUnderlyingLabel,
    leverageAtDate,
    marketBetaPolarity,
    classifyPairMatch,
    areSymmetricPair,
    reconstructTotalReturnSeries,
    yangZhangVolatility,
    studentT4CDF,
    studentT5CDF,
    normalQuantile,
    normalCDF,
    rollingAbdiRanaldo21,
    rollingCorwinSchultz21,
    holderPowerMean,
    benjaminiHochbergFDR
  });
}));
