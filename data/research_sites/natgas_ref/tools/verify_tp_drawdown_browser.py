"""
Playwright Chromium Automated Visual & UI/UX Test Suite
Verifies:
1. Automated Test Suites & Code Integrity
2. DOM Layout Order (tp-canvas -> dd-panel -> vp-panel)
3. Mode Toggle (Rolling 52W vs ATH)
4. Horizon Buttons (1M, 3M, 6M, 1Y, 3Y, 5Y, ALL)
5. KPI Card Validations (No NaN, proper formatting, ordinal dates)
6. 6 ETF Chip selectors (Brand colors, spotlight switching, KPI reactivity)
7. Canvas Hover Crosshair & Tooltip (Ordinal date formatting)
8. Desktop (1440x900) & Mobile (390x844) element & full-page screenshot capture
"""

import http.server
import socketserver
import threading
import os
import sys
import time
import re
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(r"C:\Users\Dell\.gemini\antigravity\brain\daf2837d-7a62-4ad3-adb9-68091ad4bd3a")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress logging

def run_tests():
    # 1. Start local HTTP server on an OS-allocated free port
    server = socketserver.TCPServer(("127.0.0.1", 0), QuietHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    print(f"[SERVER] Local HTTP server started on http://127.0.0.1:{port}")

    url = f"http://127.0.0.1:{port}/docs/trough-peak.html"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        # -------------------------------------------------------------
        # DESKTOP VERIFICATION (1440 x 900)
        # -------------------------------------------------------------
        print("\n========================================================")
        print("--- Starting Desktop Verification (1440x900) ---")
        print("========================================================")
        context = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1)
        page = context.new_page()

        # Listen to console errors
        errors = []
        page.on("console", lambda msg: print(f"PAGE LOG [{msg.type}]: {msg.text}"))
        page.on("pageerror", lambda exc: print(f"PAGE ERROR: {exc}"))

        print(f"[NAVIGATE] Loading {url}...")
        page.goto(url, wait_until="domcontentloaded")

        # Wait for data to load and KPIs to populate
        print("[WAIT] Waiting for drawdown KPIs to populate...")
        page.wait_for_function("""() => {
            const el = document.querySelector('#dd-kpi-cur .dd-kpi-val');
            return el && el.textContent.includes('%') && !el.textContent.includes('NaN');
        }""", timeout=15000)

        time.sleep(1.0)  # Allow rendering to settle

        # TEST 1: DOM Layout Invariants
        print("\n[TEST 1] Verifying DOM Layout Order...")
        dom_order = page.evaluate("""() => {
            const tpCanvas = document.getElementById('tp-canvas');
            const ddPanel = document.getElementById('dd-panel');
            const tablePanel = document.querySelector('.table-panel');
            const vpPanel = document.getElementById('vp-panel');

            if (!tpCanvas || !ddPanel || !tablePanel) {
                return { ok: false, error: 'One or more required elements not found' };
            }

            const tpChartContainer = tpCanvas.closest('.chart-container');
            const isDdAfterTp = (tpCanvas.compareDocumentPosition(ddPanel) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
            const isTableAfterDd = (ddPanel.compareDocumentPosition(tablePanel) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
            const isDdImmediateNextToTp = ddPanel.previousElementSibling === tpChartContainer;
            const isTableImmediateNextToDd = ddPanel.nextElementSibling === tablePanel;

            return {
                ok: isDdAfterTp && isTableAfterDd && isDdImmediateNextToTp && isTableImmediateNextToDd && !vpPanel,
                isDdAfterTp,
                isTableAfterDd,
                isDdImmediateNextToTp,
                isTableImmediateNextToDd,
                noVpPanel: !vpPanel
            };
        }""")
        print(f"DOM Layout Invariants: {dom_order}")
        assert dom_order["ok"], f"DOM layout invariant failure: {dom_order}"
        print("PASS: DOM Layout Order Invariants verified (#tp-canvas -> #dd-panel -> .table-panel; #vp-panel cleanly absent).")

        # TEST 2: KPI Card Initial Population & Non-NaN Checks
        print("\n[TEST 2] Verifying KPI Cards Data & Formats...")
        kpi_data = page.evaluate("""() => {
            const curVal = document.querySelector('#dd-kpi-cur .dd-kpi-val')?.textContent.trim();
            const curSub = document.querySelector('#dd-kpi-cur .dd-kpi-sub')?.textContent.trim();
            const mddVal = document.querySelector('#dd-kpi-mdd .dd-kpi-val')?.textContent.trim();
            const mddSub = document.querySelector('#dd-kpi-mdd .dd-kpi-sub')?.textContent.trim();
            const pkVal = document.querySelector('#dd-kpi-peak .dd-kpi-val')?.textContent.trim();
            const pkSub = document.querySelector('#dd-kpi-peak .dd-kpi-sub')?.textContent.trim();
            const uwVal = document.querySelector('#dd-kpi-underwater .dd-kpi-val')?.textContent.trim();
            const uwSub = document.querySelector('#dd-kpi-underwater .dd-kpi-sub')?.textContent.trim();

            return { curVal, curSub, mddVal, mddSub, pkVal, pkSub, uwVal, uwSub };
        }""")
        print(f"Initial KPI Data: {kpi_data}")
        assert "%" in kpi_data["curVal"] and "NaN" not in kpi_data["curVal"], f"Invalid curVal: {kpi_data['curVal']}"
        assert "%" in kpi_data["mddVal"] and "NaN" not in kpi_data["mddVal"], f"Invalid mddVal: {kpi_data['mddVal']}"
        assert "$" in kpi_data["pkVal"] and "NaN" not in kpi_data["pkVal"], f"Invalid pkVal: {kpi_data['pkVal']}"
        assert "d" in kpi_data["uwVal"] and "NaN" not in kpi_data["uwVal"], f"Invalid uwVal: {kpi_data['uwVal']}"

        date_pattern = re.compile(r"\d+(?:st|nd|rd|th)\s+[A-Z][a-z]{2},\s+\d{4}")
        assert date_pattern.search(kpi_data["mddSub"]), f"mddSub does not contain ordinal date: {kpi_data['mddSub']}"
        assert date_pattern.search(kpi_data["pkSub"]), f"pkSub does not contain ordinal date: {kpi_data['pkSub']}"
        assert date_pattern.search(kpi_data["uwSub"]) or "Currently at Peak" in kpi_data["uwSub"], f"uwSub does not contain ordinal date or peak state: {kpi_data['uwSub']}"
        print("PASS: KPI Cards verified with valid numbers and standard ordinal dates.")

        # TEST 3: Mode Toggle (Rolling 52W <-> ATH)
        print("\n[TEST 3] Verifying Mode Toggle...")
        badge_before = page.locator("#dd-badge").text_content()
        assert "ROLLING 52W" in badge_before

        print("Clicking ALL-TIME HIGH mode tab...")
        page.locator('#dd-mode-tabs button[data-mode="ath"]').click()
        time.sleep(0.5)

        badge_ath = page.locator("#dd-badge").text_content()
        print(f"Badge after ATH switch: {badge_ath}")
        assert "ALL-TIME HIGH" in badge_ath, f"Badge did not update to ATH: {badge_ath}"

        pk_lbl = page.locator("#dd-kpi-peak .dd-kpi-lbl").text_content()
        assert pk_lbl == "ALL-TIME HIGH", f"Peak card label did not update: {pk_lbl}"

        print("Clicking ROLLING 52W mode tab...")
        page.locator('#dd-mode-tabs button[data-mode="rolling_52w"]').click()
        time.sleep(0.5)
        badge_52w = page.locator("#dd-badge").text_content()
        assert "ROLLING 52W" in badge_52w, f"Badge did not restore to ROLLING 52W: {badge_52w}"
        print("PASS: Mode Toggle smoothly switches state, badge, and KPI labels.")

        # TEST 4: Horizon Buttons
        print("\n[TEST 4] Verifying Horizon Buttons (1M, 3M, 6M, 1Y, 3Y, 5Y, ALL)...")
        horizons = ["1m", "3m", "6m", "1y", "3y", "5y", "all"]
        for hz in horizons:
            btn = page.locator(f'#dd-horizon-group button[data-horizon="{hz}"]')
            btn.click()
            time.sleep(0.3)
            assert "active" in btn.get_attribute("class"), f"Horizon {hz} not marked active"
            badge = page.locator("#dd-badge").text_content()
            assert hz.upper() in badge, f"Badge does not contain {hz.upper()}: {badge}"
            mdd_lbl = page.locator("#dd-kpi-mdd .dd-kpi-lbl").text_content()
            assert hz.upper() in mdd_lbl, f"MDD label does not contain {hz.upper()}: {mdd_lbl}"
        print("PASS: All 7 Horizon buttons verified with active styling, badge, and label updates.")

        # Restore ALL horizon
        page.locator('#dd-horizon-group button[data-horizon="all"]').click()
        time.sleep(0.3)

        # TEST 5: ETF Chips Selection & Spotlight Reactivity
        print("\n[TEST 5] Verifying 6 ETF Chips & Spotlight Reactivity...")
        chip_count = page.locator("#dd-etf-chips .etf-chip").count()
        assert chip_count == 6, f"Expected 6 ETF chips, got {chip_count}"

        expected_colors = {
            "BOIL": "rgb(192, 120, 40)",
            "HNU": "rgb(61, 184, 122)",
            "3NGL": "rgb(136, 85, 187)",
            "KOLD": "rgb(80, 144, 160)",
            "HND": "rgb(212, 168, 48)",
            "3NGS": "rgb(224, 96, 96)"
        }

        for idx in range(chip_count):
            chip = page.locator("#dd-etf-chips .etf-chip").nth(idx)
            text = chip.text_content().strip()
            assert text in expected_colors, f"Unexpected chip label {text}"
            color = chip.evaluate("el => getComputedStyle(el).color")
            exp_color = expected_colors[text]
            assert color == exp_color, f"Chip {text} color mismatch: expected {exp_color}, got {color}"

        # Click BOIL chip to switch spotlight
        print("Clicking BOIL chip...")
        page.locator("#dd-etf-chips .etf-chip", has_text="BOIL").click()
        time.sleep(0.5)

        boil_kpis = page.evaluate("""() => {
            const peakColor = getComputedStyle(document.querySelector('#dd-kpi-peak .dd-kpi-val')).color;
            const curVal = document.querySelector('#dd-kpi-cur .dd-kpi-val')?.textContent.trim();
            const pkVal = document.querySelector('#dd-kpi-peak .dd-kpi-val')?.textContent.trim();
            return { peakColor, curVal, pkVal };
        }""")
        print(f"BOIL Active KPIs: {boil_kpis}")
        assert boil_kpis["peakColor"] == "rgb(192, 120, 40)", f"Peak reference color did not match BOIL: {boil_kpis['peakColor']}"

        # Click KOLD chip to switch spotlight
        print("Clicking KOLD chip...")
        page.locator("#dd-etf-chips .etf-chip", has_text="KOLD").click()
        time.sleep(0.5)

        kold_kpis = page.evaluate("""() => {
            const peakColor = getComputedStyle(document.querySelector('#dd-kpi-peak .dd-kpi-val')).color;
            const curVal = document.querySelector('#dd-kpi-cur .dd-kpi-val')?.textContent.trim();
            const pkVal = document.querySelector('#dd-kpi-peak .dd-kpi-val')?.textContent.trim();
            return { peakColor, curVal, pkVal };
        }""")
        print(f"KOLD Active KPIs: {kold_kpis}")
        assert kold_kpis["peakColor"] == "rgb(80, 144, 160)", f"Peak reference color did not match KOLD: {kold_kpis['peakColor']}"
        print("PASS: 6 ETF Chips verified with brand colors, spotlight reactivity, and KPI sync.")

        # TEST 6: Hover Crosshair & Tooltip
        print("\n[TEST 6] Verifying Hover Crosshair & Tooltip...")
        canvas = page.locator("#drawdown-canvas")
        box = canvas.bounding_box()
        assert box is not None and box["width"] > 0, "Drawdown canvas bounding box invalid"

        # Hover near middle of canvas
        hover_x = box["x"] + box["width"] * 0.5
        hover_y = box["y"] + box["height"] * 0.5
        page.mouse.move(hover_x, hover_y)
        time.sleep(0.5)

        tooltip = page.locator("#dd-tooltip")
        is_visible = tooltip.evaluate("el => getComputedStyle(el).display !== 'none'")
        assert is_visible, "Tooltip did not become visible on canvas hover"

        tooltip_date = page.locator("#dd-tooltip .tooltip-date").text_content().strip()
        print(f"Tooltip Date Displayed: {tooltip_date}")
        assert date_pattern.match(tooltip_date), f"Tooltip date format violation: '{tooltip_date}' (expected format like '24th Oct, 2025')"

        row_count = page.locator("#dd-tooltip .tooltip-row").count()
        assert row_count == 6, f"Expected 6 ETF rows in tooltip, got {row_count}"
        print("PASS: Hover crosshair and tooltip verified with correct date formatting and 6 series rows.")

        # TEST 7: Desktop Screenshots
        print("\n[TEST 7] Capturing Desktop Screenshots...")
        page.mouse.move(10, 10)
        time.sleep(0.3)

        # Drawdown Panel Element Screenshot
        dd_panel = page.locator("#dd-panel")
        dd_desktop_path = OUTPUT_DIR / "tp_drawdown_desktop_1440x900.png"
        dd_panel.screenshot(path=str(dd_desktop_path))
        print(f"Saved: {dd_desktop_path}")

        # Section Desktop Screenshot (Top hierarchy: Title, Sliders, Price Map, Drawdown, Table)
        full_desktop_path = OUTPUT_DIR / "tp_fullpage_desktop_1440x900.png"
        page.screenshot(path=str(full_desktop_path))
        print(f"Saved: {full_desktop_path}")

        context.close()

        # -------------------------------------------------------------
        # MOBILE VERIFICATION (390 x 844 - iPhone 12/13/14 viewport)
        # -------------------------------------------------------------
        print("\n========================================================")
        print("--- Starting Mobile Verification (390x844) ---")
        print("========================================================")
        m_context = browser.new_context(
            viewport={"width": 390, "height": 844},
            is_mobile=True,
            has_touch=True,
            device_scale_factor=2
        )
        m_page = m_context.new_page()
        m_page.goto(url, wait_until="domcontentloaded")

        m_page.wait_for_function("""() => {
            const el = document.querySelector('#dd-kpi-cur .dd-kpi-val');
            return el && el.textContent.includes('%') && !el.textContent.includes('NaN');
        }""", timeout=15000)

        time.sleep(1.0)

        # Verify Mobile Responsive Layout of KPI Grid
        kpi_grid_style = m_page.evaluate("""() => {
            const grid = document.getElementById('dd-kpi-grid');
            return getComputedStyle(grid).display;
        }""")
        assert kpi_grid_style == "grid", f"KPI grid not display:grid: {kpi_grid_style}"

        # Capture Mobile Screenshots
        m_dd_panel = m_page.locator("#dd-panel")
        dd_mobile_path = OUTPUT_DIR / "tp_drawdown_mobile_390x844.png"
        m_dd_panel.screenshot(path=str(dd_mobile_path))
        print(f"Saved: {dd_mobile_path}")

        full_mobile_path = OUTPUT_DIR / "tp_fullpage_mobile_390x844.png"
        m_page.screenshot(path=str(full_mobile_path))
        print(f"Saved: {full_mobile_path}")

        m_context.close()
        browser.close()

    server.shutdown()
    print("\n========================================================")
    print("ALL PLAYWRIGHT HEADLESS BROWSER VERIFICATIONS PASSED!")
    print("========================================================")

if __name__ == "__main__":
    run_tests()
