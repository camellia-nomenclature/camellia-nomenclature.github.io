"""
End-to-end tests for camellia-nomenclature.github.io
Requires: pip install playwright && playwright install chromium
Run against local server: python3 -m http.server 8081
Usage: python3 test_site.py
"""
from playwright.sync_api import sync_playwright, expect

BASE = "http://localhost:8081"
TIMEOUT = 8000  # ms for card to render


def make_page(browser):
    page = browser.new_page()
    js_errors = []
    page.on("pageerror", lambda e: js_errors.append(str(e)))
    # Abort external image fetches so they don't block or spin indefinitely
    page.route("**/*.jpg", lambda r: r.abort())
    page.route("**/*.png", lambda r: r.abort())
    page.route("**/*.gif", lambda r: r.abort())
    page.route("**/*.webp", lambda r: r.abort())
    return page, js_errors


def test_kwan_yuen_special():
    with sync_playwright() as p:
        browser = p.chromium.launch()

        # --- Test 1: direct hash navigation ---
        print("Test 1: direct hash navigation to #Kwan%20Yuen%20Special")
        page, js_errors = make_page(browser)
        page.goto(f"{BASE}/#Kwan%20Yuen%20Special", wait_until="commit", timeout=10000)
        # Both the header AND body must be fully rendered (not just loading state)
        expect(page.locator(".card-body")).not_to_contain_text("Loading details...", timeout=TIMEOUT)
        expect(page.locator(".card-header h2")).to_have_text("Kwan Yuen Special", timeout=TIMEOUT)
        body_text = page.locator(".card-body").inner_text()
        assert "Reticulata" in body_text, f"Expected species in card body, got: {body_text[:200]}"
        assert not js_errors, f"JS errors: {js_errors}"
        print("  PASS: card rendered, species present, no JS errors")

        # --- Test 2: search-then-click ---
        print("Test 2: search-then-click flow")
        page, js_errors = make_page(browser)
        page.goto(BASE, wait_until="commit", timeout=10000)
        page.fill("#search", "Kwan Yuen Special")
        page.locator(".dropdown-item").first.click()
        expect(page.locator(".card-header h2")).to_have_text("Kwan Yuen Special", timeout=TIMEOUT)
        expect(page.locator(".card-body")).not_to_contain_text("Loading details...", timeout=TIMEOUT)
        assert not js_errors, f"JS errors: {js_errors}"
        print("  PASS: card rendered after search+click")

        # --- Test 3: missing entry does not hang ---
        print("Test 3: missing entry does not hang on 'Loading details...'")
        page, js_errors = make_page(browser)
        page.goto(f"{BASE}/#__nonexistent_cultivar__", wait_until="commit", timeout=10000)
        page.wait_for_timeout(2500)
        loading = page.locator(".card-body").filter(has_text="Loading details...")
        assert loading.count() == 0, "Page stuck on 'Loading details...' for missing entry"
        assert not js_errors, f"JS errors: {js_errors}"
        print("  PASS: no infinite hang for missing entry")

        browser.close()
        print("\nAll tests passed.")


if __name__ == "__main__":
    test_kwan_yuen_special()
