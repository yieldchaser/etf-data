"""Regression tests for Excel cell-type coercion and month-key seam integrity
in conviction.ingest_markets_xl.

Prior audits flagged but never verified:
  * Excel serial-number dates collapsing into a phantom 1970-01 month that
    then overwrites real history at the merge seam (Excel priority on
    overlap) — serial ints parse as epoch NANOSECONDS under pd.to_datetime.
  * Numeric-text serials ('46000') coerced to NaT, silently dropping whole
    series.
  * Comma-thousands text closes ('5,123.45') raising TypeError in the
    Close > 0 filter, killing the entire sheet read.
  * --dry-run being ignored on the --sync-excel side-channel (workbook
    written despite the dry run).
  * _read_mega_xl returning None (not {}) when the workbook fails to open.

The month-key seam itself is pinned too: first-of-month, mid-month and
end-of-month stamps must all bucket to the same YYYY-MM key, so Excel-built
months line up 1:1 with FRED-built months and the merge neither double-counts
nor drops months at the stitch boundary.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import conviction.ingest_markets_xl as ixl
from conviction.ingest_markets_xl import (
    _coerce_close,
    _coerce_excel_dates,
    _is_lfs_pointer,
    _read_mega_xl,
    _to_monthly_eop,
    merge_monthly,
)


def _serial(ts: str) -> int:
    """Excel serial day number for a calendar date (epoch 1899-12-30)."""
    return int((pd.Timestamp(ts) - pd.Timestamp("1899-12-30")).days)


# ─── 1. Date coercion: serial numbers vs datetime cells ──────────────────────

class TestExcelDateCoercion:
    def test_serial_int_dates_map_to_calendar_months(self):
        s = pd.Series([_serial("2020-01-31"), _serial("2020-02-29")])
        out = _coerce_excel_dates(s)
        assert [t.strftime("%Y-%m") for t in out] == ["2020-01", "2020-02"]

    def test_serial_int_dates_never_collapse_into_1970(self):
        s = pd.Series([_serial("2020-01-31"), _serial("2020-02-29")])
        out = _coerce_excel_dates(s)
        assert not out.dt.strftime("%Y-%m").eq("1970-01").any()

    def test_serial_text_dates_are_not_dropped(self):
        s = pd.Series(["46000", " 46031 "])
        out = _coerce_excel_dates(s)
        assert out.notna().all()
        expected = (pd.Timestamp("1899-12-30") + pd.Timedelta(days=46000)).strftime("%Y-%m")
        assert out.iloc[0].strftime("%Y-%m") == expected

    def test_datetime_cells_pass_through_untouched(self):
        s = pd.Series(pd.to_datetime(["2026-03-31", "2026-04-30"]))
        out = _coerce_excel_dates(s)
        assert pd.api.types.is_datetime64_any_dtype(out)
        assert [t.strftime("%Y-%m") for t in out] == ["2026-03", "2026-04"]

    def test_mixed_datetime_and_serial_cells(self):
        s = pd.Series([pd.Timestamp("2026-01-31"), _serial("2026-02-28")], dtype=object)
        out = _coerce_excel_dates(s)
        assert [t.strftime("%Y-%m") for t in out] == ["2026-01", "2026-02"]

    def test_out_of_range_serials_become_nat_and_drop(self):
        s = pd.Series([999_999_999, _serial("2021-06-30")])
        out = _coerce_excel_dates(s)
        assert pd.isna(out.iloc[0])
        assert out.iloc[1].strftime("%Y-%m") == "2021-06"

    def test_unparseable_text_stays_nat(self):
        out = _coerce_excel_dates(pd.Series(["n/a", ""]))
        assert out.isna().all()


# ─── 2. Close coercion: commas-as-text and currency markers ──────────────────

class TestCloseCoercion:
    def test_comma_thousands_text_closes_coerce(self):
        out = _coerce_close(pd.Series(["5,123.45", "6,000.10"]))
        assert out.tolist() == [5123.45, 6000.10]

    def test_multi_comma_groups_coerce(self):
        assert _coerce_close(pd.Series(["1,234,567.89"])).iloc[0] == 1234567.89

    def test_currency_prefixed_closes_coerce(self):
        out = _coerce_close(pd.Series(["$1,234.56", "£987.6", "€55"]))
        assert out.tolist() == [1234.56, 987.6, 55.0]

    def test_plain_numeric_dtype_passthrough(self):
        out = _coerce_close(pd.Series([100, 200.5]))
        assert out.tolist() == [100.0, 200.5]

    def test_unparseable_close_becomes_nan_not_crash(self):
        out = _coerce_close(pd.Series(["n/a", "100"]))
        assert pd.isna(out.iloc[0])
        assert out.iloc[1] == 100.0

    def test_comma_text_no_longer_crashes_gt_zero_filter(self):
        s = _coerce_close(pd.Series(["5,123.45", "0.5"]))
        assert (s > 0).all()


# ─── 3. Month-key seam: stamp-position independence + merge precedence ───────

class TestMonthKeySeam:
    def test_first_mid_and_end_of_month_stamp_to_same_key(self):
        """EOP vs first-of-month labelling must not shift the YYYY-MM key —
        a systematic off-by-one here double-counts or drops months at the
        Excel↔FRED merge seam."""
        eom = _to_monthly_eop(pd.DataFrame({
            "Date": pd.to_datetime(["2026-05-31"]),
            "Close": [12.0],
        }))
        fom = _to_monthly_eop(pd.DataFrame({
            "Date": pd.to_datetime(["2026-05-01"]),
            "Close": [10.0],
        }))
        mid = _to_monthly_eop(pd.DataFrame({
            "Date": pd.to_datetime(["2026-05-15"]),
            "Close": [11.0],
        }))
        assert [r[0] for r in eom + fom + mid] == ["2026-05"] * 3

    def test_daily_rows_bucket_into_one_eop_month(self):
        df = pd.DataFrame({
            "Date": pd.date_range("2026-05-01", "2026-05-31", freq="D"),
            "Close": range(1, 32),
        })
        out = _to_monthly_eop(df)
        assert out == [["2026-05", 31.0]]

    def test_excel_wins_on_overlap(self):
        """Excel-priority overwrite rule: the incoming (Excel) series wins for
        overlapping months; existing months outside the overlap survive."""
        existing = [["2026-03", 111.0], ["2026-04", 222.0]]
        new = [["2026-03", 999.5]]
        merged = merge_monthly(existing, new, asset_id="gold")
        assert merged == [["2026-03", 999.5], ["2026-04", 222.0]]

    def test_merge_is_union_with_no_duplicate_keys(self):
        existing = [["1990-01", 100.0], ["1990-02", 101.0]]
        new = [["1871-01", 4.44], ["1990-01", 150.0]]
        merged = merge_monthly(existing, new, asset_id="gold")
        keys = [r[0] for r in merged]
        assert keys == sorted(keys)
        assert len(keys) == len(set(keys)) == 3
        assert dict((k, v) for k, v in merged)["1990-01"] == 150.0


# ─── 4. Full-pipeline read: serial-date cells through _read_mega_xl ──────────

class TestReadMegaXlCoercion:
    def _write_sheet(self, path: Path, rows: list[tuple]) -> None:
        openpyxl = pytest.importorskip("openpyxl")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Equities"
        ws.append(["Date", "Index_Name", "Close"])
        for r in rows:
            ws.append(list(r))
        wb.save(path)

    def test_serial_date_cells_produce_correct_months(self, tmp_path):
        xl = tmp_path / "Mega.xlsx"
        self._write_sheet(xl, [
            (_serial("1970-01-30"), "S&P 500", 90.0),   # the real 1970-01 point
            (_serial("2020-01-31"), "S&P 500", 3200.0),
            (_serial("2020-02-29"), "S&P 500", 3300.0),
        ])
        raw = _read_mega_xl(xl)
        assert "sp500" in raw
        monthly = _to_monthly_eop(raw["sp500"], asset_id="sp500")
        keys = [r[0] for r in monthly]
        assert keys == ["1970-01", "2020-01", "2020-02"], keys
        assert not any(k.startswith("1970-01") and k != "1970-01" for k in keys)

    def test_serial_dates_do_not_overwrite_real_1970_at_seam(self, tmp_path):
        """The phantom-1970 failure mode: with Excel priority, a garbage
        1970-01 serial bucket would overwrite the genuine 1970-01 close."""
        xl = tmp_path / "Mega.xlsx"
        self._write_sheet(xl, [
            (_serial("1970-01-30"), "S&P 500", 90.0),
            (_serial("2020-01-31"), "S&P 500", 3200.0),
        ])
        monthly = _to_monthly_eop(_read_mega_xl(xl)["sp500"])
        assert dict(monthly)["1970-01"] == 90.0

    def test_comma_text_closes_read_through_pipeline(self, tmp_path):
        xl = tmp_path / "Mega.xlsx"
        self._write_sheet(xl, [
            ("2020-01-31", "S&P 500", "3,200.00"),
            ("2020-02-29", "S&P 500", "3,300.55"),
        ])
        raw = _read_mega_xl(xl)
        monthly = _to_monthly_eop(raw["sp500"])
        assert monthly == [["2020-01", 3200.0], ["2020-02", 3300.55]]

    def test_workbook_open_failure_returns_empty_dict(self, tmp_path, monkeypatch):
        xl = tmp_path / "Mega.xlsx"
        xl.write_bytes(b"x" * 2048)  # >1KB so the LFS-pointer guard passes

        def _boom(*a, **k):
            raise RuntimeError("corrupt workbook")

        monkeypatch.setattr(pd, "ExcelFile", _boom)
        result = _read_mega_xl(xl)
        assert result == {}  # regression: was None (bare return)

    def test_missing_sheet_is_skipped_not_fatal(self, tmp_path):
        xl = tmp_path / "Mega.xlsx"
        openpyxl = pytest.importorskip("openpyxl")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Totally_Other_Sheet"
        ws.append(["Date", "Index_Name", "Close"])
        wb.save(xl)
        raw = _read_mega_xl(xl)
        assert raw == {}

    def test_lfs_pointer_and_empty_stub_detected(self, tmp_path):
        empty = tmp_path / "empty.xlsx"
        empty.write_bytes(b"")
        pointer = tmp_path / "pointer.xlsx"
        pointer.write_bytes(b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\n")
        real = tmp_path / "real.xlsx"
        real.write_bytes(b"x" * 2048)
        assert _is_lfs_pointer(empty)
        assert _is_lfs_pointer(pointer)
        assert not _is_lfs_pointer(real)


# ─── 5. dry_run must win over --sync-excel ───────────────────────────────────

class TestDryRunSyncGuard:
    def _workbook_with_row(self, path: Path) -> None:
        openpyxl = pytest.importorskip("openpyxl")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Equities"
        ws.append(["Date", "Index_Name", "Close", "Frequency"])
        ws.append(["2019-12-01", "S&P 500", 3141.63, "monthly"])
        wb.save(path)

    def _output_data(self) -> dict:
        return {
            "assets": {
                "sp500": {
                    "meta": {},
                    "monthly": [["2019-11", 3140.98], ["2019-12", 3141.63]],
                }
            }
        }

    def test_sync_dry_run_does_not_write(self, tmp_path):
        xl = tmp_path / "Mega.xlsx"
        self._workbook_with_row(xl)
        before = xl.read_bytes()
        added = ixl.sync_completed_months_to_excel(
            xl_path=xl, output_data=self._output_data(), dry_run=True
        )
        assert added == 1  # plan counted 2019-11
        assert xl.read_bytes() == before  # but the workbook is untouched

    def test_sync_without_dry_run_appends(self, tmp_path):
        xl = tmp_path / "Mega.xlsx"
        self._workbook_with_row(xl)
        added = ixl.sync_completed_months_to_excel(
            xl_path=xl, output_data=self._output_data(), dry_run=False
        )
        assert added == 1
        openpyxl = pytest.importorskip("openpyxl")
        wb = openpyxl.load_workbook(xl)
        rows = list(wb["Equities"].iter_rows(min_row=2, values_only=True))
        assert any(r[0] == "2019-11-01" for r in rows)

    def test_main_dry_run_flag_guards_sync(self, monkeypatch):
        captured: dict = {}

        def _fake_sync(*args, **kwargs):
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(ixl, "sync_completed_months_to_excel", _fake_sync)
        ixl.main(["--dry-run", "--sync-excel"])
        assert captured.get("dry_run") is True

        ixl.main(["--sync-excel"])
        assert captured.get("dry_run") is False


# ─── 6. process() dry_run and freshness-gate behaviour ───────────────────────

class TestProcessDryRunAndFreshness:
    def test_dry_run_never_writes_output_json(self, tmp_path, monkeypatch):
        """With the Excel stubbed out as an LFS pointer, process(dry_run=True)
        must return a plan WITHOUT creating market_returns.json."""
        monkeypatch.setattr(ixl, "MEGA_XL", tmp_path / "stub.xlsx")
        stub = tmp_path / "stub.xlsx"
        stub.write_bytes(b"version https://git-lfs.github.com/spec/v1\n")
        monkeypatch.setattr(ixl, "MARKETS1_XL", tmp_path / "nope_1.xlsx")
        out_path = tmp_path / "market_returns.json"
        monkeypatch.setattr(ixl, "OUTPUT_PATH", out_path)

        result = ixl.process(dry_run=True, merge=False, fail_on_stale=False)
        assert isinstance(result, dict) and "assets" in result
        assert not out_path.exists()

    def test_fail_on_stale_exits_nonzero(self, tmp_path, capsys):
        ancient = {
            "asof": "2000-01",
            "assets": {
                "gold": {
                    "meta": {"name": "Gold", "category": "precious_metals", "last": "2000-01"},
                    "monthly": [["2000-01", 280.0]],
                }
            },
            "fx": {}, "cpi": {}, "rates": {}, "events": [],
        }
        with pytest.raises(SystemExit) as exc:
            ixl.check_freshness(ancient, fail_on_stale=True)
        assert exc.value.code == 1

    def test_no_fail_on_stale_warns_and_continues(self):
        ancient = {
            "asof": "2000-01",
            "assets": {
                "gold": {
                    "meta": {"name": "Gold", "category": "precious_metals", "last": "2000-01"},
                    "monthly": [["2000-01", 280.0]],
                }
            },
            "fx": {}, "cpi": {}, "rates": {}, "events": [],
        }
        stale = ixl.check_freshness(ancient, fail_on_stale=False)
        assert any("gold" in s for s in stale)

    def test_fresh_series_passes_gate(self):
        fresh = {
            "asof": pd.Timestamp.today().strftime("%Y-%m"),
            "assets": {
                "gold": {
                    "meta": {
                        "name": "Gold", "category": "precious_metals",
                        "last": pd.Timestamp.today().strftime("%Y-%m"),
                    },
                    "monthly": [],
                }
            },
            "fx": {}, "cpi": {}, "rates": {}, "events": [],
        }
        assert ixl.check_freshness(fresh, fail_on_stale=True) == []
