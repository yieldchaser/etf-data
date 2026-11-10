import os
import sys
import time
import subprocess
import threading
import http.server
import socketserver
from playwright.sync_api import sync_playwright

PORT = 8991
DIRECTORY = os.path.abspath("docs")

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)
    def log_message(self, format, *args):
        pass # Silence logging

def start_server():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        httpd.serve_forever()

t = threading.Thread(target=start_server, daemon=True)
t.start()
time.sleep(1)

BASE_URL = f"http://127.0.0.1:{PORT}/index.html"
ARTIFACT_DIR = "C:/Users/Dell/.gemini/antigravity/brain/daf2837d-7a62-4ad3-adb9-68091ad4bd3a"

console_errors = []

def is_app_error(msg):
    txt = str(msg)
    # Ignore live network fetch / proxy failures when running local offline server
    if "401" in txt or "proxy" in txt.lower() or "aggregateerror" in txt.lower():
        return False
    return True

with sync_playwright() as p:
    browser = p.chromium.launch()
    context = browser.new_context(viewport={"width": 1600, "height": 1100}, device_scale_factor=2)
    page = context.new_page()

    page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" and is_app_error(msg.text) else None)
    page.on("pageerror", lambda err: console_errors.append(str(err)) if is_app_error(err) else None)

    print(f"Loading {BASE_URL}...")
    page.goto(BASE_URL, wait_until="load", timeout=30000)
    page.wait_for_selector("#heat-calendar", timeout=10000)
    page.wait_for_timeout(1500)

    # 1. Baseline view screenshot
    heat_card = page.locator("#heat-calendar-panel")
    heat_card.screenshot(path=os.path.join(ARTIFACT_DIR, "heatmap_baseline.png"))
    print("Baseline screenshot captured.")

    # 2. Click PEAKS toggle
    peaks_btn = page.locator("#heat-peaks-toggle")
    peaks_btn.click()
    page.wait_for_timeout(500)

    # 3. Click SHORT side filter
    short_btn = page.locator("#heat-side-group button[data-side='short']")
    short_btn.click()
    page.wait_for_timeout(500)

    badge = page.locator("#heat-peak-count")
    # Default threshold is 95
    p95_count_text = badge.inner_text()
    print(f"Short 90D at P95: {p95_count_text}")
    assert "5 PEAKS" in p95_count_text, f"Expected 5 PEAKS at P95, got {p95_count_text}"

    # Capture P95 screenshot
    heat_card.screenshot(path=os.path.join(ARTIFACT_DIR, "heatmap_short_p95.png"))

    # 4. Click P99 threshold
    p99_btn = page.locator("#heat-peaks-threshold-group button[data-threshold='99']")
    p99_btn.click()
    page.wait_for_timeout(500)

    p99_count_text = badge.inner_text()
    print(f"Short 90D at P99: {p99_count_text}")
    assert "1 PEAK" in p99_count_text, f"Expected 1 PEAK at P99, got {p99_count_text}"

    heat_card.screenshot(path=os.path.join(ARTIFACT_DIR, "heatmap_short_p99.png"))

    # 5. Click P90 threshold
    p90_btn = page.locator("#heat-peaks-threshold-group button[data-threshold='90']")
    p90_btn.click()
    page.wait_for_timeout(500)

    p90_count_text = badge.inner_text()
    print(f"Short 90D at P90: {p90_count_text}")
    assert "9 PEAKS" in p90_count_text, f"Expected 9 PEAKS at P90, got {p90_count_text}"

    heat_card.screenshot(path=os.path.join(ARTIFACT_DIR, "heatmap_short_p90.png"))

    # 6. Click VALLEYS toggle
    valleys_btn = page.locator("#heat-valleys-toggle")
    valleys_btn.click()
    page.wait_for_timeout(500)

    valleys_count_text = badge.inner_text()
    print(f"Short 90D Valleys at P90: {valleys_count_text}")
    assert "VALLEYS" in valleys_count_text or "VALLEY" in valleys_count_text

    heat_card.screenshot(path=os.path.join(ARTIFACT_DIR, "heatmap_short_valleys.png"))

    # 7. Check 252 days view
    days252_btn = page.locator("#heat-days-group button[data-days='252']")
    days252_btn.click()
    page.wait_for_timeout(500)

    # Click Peaks on 252D
    peaks_btn.click()
    page.wait_for_timeout(500)

    # Test P99 on 252D
    p99_btn.click()
    page.wait_for_timeout(500)
    p99_252 = badge.inner_text()
    print(f"Short 252D at P99: {p99_252}")
    assert "3 PEAKS" in p99_252, f"Expected 3 PEAKS at P99 (252D), got {p99_252}"

    # Test P95 on 252D
    p95_btn = page.locator("#heat-peaks-threshold-group button[data-threshold='95']")
    p95_btn.click()
    page.wait_for_timeout(500)
    p95_252 = badge.inner_text()
    print(f"Short 252D at P95: {p95_252}")
    assert "13 PEAKS" in p95_252, f"Expected 13 PEAKS at P95 (252D), got {p95_252}"

    # Test P90 on 252D
    p90_btn.click()
    page.wait_for_timeout(500)
    p90_252 = badge.inner_text()
    print(f"Short 252D at P90: {p90_252}")
    assert "25 PEAKS" in p90_252, f"Expected 25 PEAKS at P90 (252D), got {p90_252}"

    heat_card.screenshot(path=os.path.join(ARTIFACT_DIR, "heatmap_short_252d_p90.png"))

    browser.close()

if console_errors:
    print(f"Console errors encountered: {console_errors}")
    sys.exit(1)

print("\nSUCCESS! All browser tests and screenshot captures passed perfectly!")
