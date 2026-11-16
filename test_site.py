"""
End-to-end checks for camellia-nomenclature.github.io. Read-only: GET requests only,
never submits forms or touches the bug-report / Firebase UI, so it is safe against prod.
Requires: pip install playwright && playwright install chromium

How to run:
  python3 -m http.server 8081          # in the repo root, in another terminal
  python3 test_site.py                 # default: BASE_URL=http://localhost:8081, TEST_BROWSER=chromium

  BASE_URL=http://127.0.0.1:18500 TEST_BROWSER=webkit python3 test_site.py
  BASE_URL=https://camellia-nomenclature.github.io/ python3 test_site.py    # prod

WebKit needs a WebKit build matching this Playwright version: either point at an existing one,
  PLAYWRIGHT_BROWSERS_PATH=/tmp/camellia-modern-web/pw-browsers TEST_BROWSER=webkit python3 test_site.py
or install it once with: python3 -m playwright install webkit
(PLAYWRIGHT_BROWSERS_PATH replaces the default browser cache, so only set it for WebKit runs.)

Image requests are aborted, as before: tests never wait on third-party photo hosts, and the
card's fixed-size image box keeps the layout identical for the overflow checks.
Any failure raises and exits nonzero; success ends with "All tests passed."
"""
import os
import sys

from playwright.sync_api import sync_playwright, expect

BASE = os.environ.get("BASE_URL", "http://localhost:8081").rstrip("/")
BROWSER = os.environ.get("TEST_BROWSER", "chromium")
if BROWSER not in ("chromium", "webkit"):
    sys.exit(f"TEST_BROWSER must be 'chromium' or 'webkit', got {BROWSER!r}")
TIMEOUT = 8000  # ms for card to render

# is_mobile/has_touch are supported by Chromium and WebKit (not Firefox). Both then match
# (pointer: coarse); Chromium also honours the page's <meta name="viewport">.
VIEWPORTS = {
    "mobile 390x844": dict(viewport={"width": 390, "height": 844}, is_mobile=True, has_touch=True),
    "desktop 1440x900": dict(viewport={"width": 1440, "height": 900}),
}
OVERFLOW_WIDTHS = (320, 390, 1440)
OVERFLOW_PAGES = ("/", "/#Kwan%20Yuen%20Special", "/#fragrant")


def make_page(context):
    page = context.new_page()
    js_errors = []
    page.on("pageerror", lambda e: js_errors.append(str(e)))
    # Abort external image fetches so they don't block or spin indefinitely
    page.route("**/*.jpg", lambda r: r.abort())
    page.route("**/*.png", lambda r: r.abort())
    page.route("**/*.gif", lambda r: r.abort())
    page.route("**/*.webp", lambda r: r.abort())
    return page, js_errors


def wait_for_data(page):
    expect(page.locator("#statsLine")).to_contain_text("indexed", timeout=TIMEOUT)


def expect_card(page, name):
    """The real detail card: .card-info only exists in a successful render (index.html:808),
    so the 'Entry not found.' and 'Failed to load entry.' cards fail this check."""
    expect(page.locator(".card-header h2")).to_have_text(name, timeout=TIMEOUT)
    expect(page.locator(".card-body .card-info")).to_be_visible(timeout=TIMEOUT)
    body = page.locator(".card-body")
    for text in ("Loading details...", "Entry not found.", "Failed to load entry."):
        expect(body).not_to_contain_text(text, timeout=TIMEOUT)


def wait_for_startup(page):
    """Startup is loadData() -> both curated lists -> navigateHash(location.hash), with no
    task boundary after the lists are assigned (index.html:965-973). Once a poll sees the
    catalogue and both lists, the initial hash routing has already run."""
    wait_for_data(page)
    page.wait_for_function(
        "camellias.length > 0 && fragrantCamellias.length > 0 && yellowCamellias.length > 0",
        timeout=TIMEOUT)


def run_checks(browser, label, opts):
    context = browser.new_context(**opts)

    # --- Test 1: direct hash navigation ---
    print(f"[{label}] Test 1: direct hash navigation to #Kwan%20Yuen%20Special")
    page, js_errors = make_page(context)
    page.goto(f"{BASE}/#Kwan%20Yuen%20Special", wait_until="commit", timeout=10000)
    # Both the header AND body must be fully rendered (not loading, not an error card)
    expect_card(page, "Kwan Yuen Special")
    body_text = page.locator(".card-body").inner_text()
    assert "Reticulata" in body_text, f"Expected species in card body, got: {body_text[:200]}"
    assert not js_errors, f"JS errors: {js_errors}"
    print("  PASS: card rendered, species present, no JS errors")

    # --- Test 2: search-then-click ---
    print(f"[{label}] Test 2: search-then-click flow")
    page, js_errors = make_page(context)
    page.goto(BASE, wait_until="commit", timeout=10000)
    wait_for_data(page)  # typing before index.json loads would never open the dropdown
    page.fill("#search", "Kwan Yuen Special")
    page.locator(".dropdown-item").first.click()
    expect_card(page, "Kwan Yuen Special")
    assert not js_errors, f"JS errors: {js_errors}"
    print("  PASS: card rendered after search+click")

    # --- Test 3: missing entry does not hang ---
    # A hash that matches no cultivar is ignored (index.html:944-946): the home view stays
    # and no card (loading or otherwise) is rendered.
    print(f"[{label}] Test 3: missing entry does not hang on 'Loading details...'")
    page, js_errors = make_page(context)
    page.goto(f"{BASE}/#__nonexistent_cultivar__", wait_until="commit", timeout=10000)
    wait_for_startup(page)
    expect(page.locator("#results .card")).to_have_count(0)
    expect(page.locator("#homeSection")).to_be_visible()
    assert not js_errors, f"JS errors: {js_errors}"
    print("  PASS: no infinite hang for missing entry (home view stays, no card)")

    # --- Test 4: smoke: type, pick from dropdown, card, back to home ---
    print(f"[{label}] Test 4: type in search, pick dropdown item, card renders, Back returns home")
    page, js_errors = make_page(context)
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_data(page)
    page.click("#search")
    page.keyboard.type("Kwan Yuen Spec", delay=20)
    page.locator(".dropdown-item", has_text="Kwan Yuen Special").first.click()
    expect_card(page, "Kwan Yuen Special")
    assert page.evaluate("location.hash") == "#Kwan%20Yuen%20Special", page.url
    page.go_back()
    expect(page.locator("#homeSection")).to_be_visible(timeout=TIMEOUT)
    expect(page.locator("#results .card")).to_have_count(0, timeout=TIMEOUT)
    assert page.evaluate("location.hash") == "", f"Expected home after Back, got {page.url}"
    assert not js_errors, f"JS errors: {js_errors}"
    print("  PASS: card rendered, Back returned to home, no JS errors")

    context.close()
    return 4


def check_overflow(browser):
    """No horizontal page scroll at 320/390/1440 px (WCAG 1.4.10 Reflow)."""
    print("Overflow: scrollWidth <= clientWidth on html and body")
    checks = 0
    for width in OVERFLOW_WIDTHS:
        opts = dict(viewport={"width": width, "height": 900 if width > 600 else 844})
        if width <= 600:
            opts.update(is_mobile=True, has_touch=True)
        context = browser.new_context(**opts)
        for path in OVERFLOW_PAGES:
            url = BASE + path
            page, js_errors = make_page(context)  # fresh page: a hash-only goto would not reload
            page.goto(url, wait_until="commit", timeout=10000)
            wait_for_data(page)
            if path.startswith("/#Kwan"):
                expect_card(page, "Kwan Yuen Special")
            elif path == "/#fragrant":
                expect(page.locator(".card-header h2")).to_contain_text("Fragrant Camellias", timeout=TIMEOUT)
            m = page.evaluate("""() => ({
                html_sw: document.documentElement.scrollWidth, html_cw: document.documentElement.clientWidth,
                body_sw: document.body.scrollWidth, body_cw: document.body.clientWidth})""")
            assert m["html_sw"] <= m["html_cw"], (
                f"Horizontal overflow on {url} at width {width}: html scrollWidth={m['html_sw']} > clientWidth={m['html_cw']}")
            assert m["body_sw"] <= m["html_cw"], (
                f"Horizontal overflow on {url} at width {width}: body scrollWidth={m['body_sw']} > "
                f"viewport clientWidth={m['html_cw']} (body clientWidth={m['body_cw']})")
            assert not js_errors, f"JS errors on {url} at width {width}: {js_errors}"
            page.close()
            checks += 1
        context.close()
        print(f"  PASS: width {width}: {', '.join(OVERFLOW_PAGES)}")
    return checks


def main():
    print(f"BASE_URL={BASE} TEST_BROWSER={BROWSER}")
    passed = 0
    with sync_playwright() as p:
        try:
            browser = getattr(p, BROWSER).launch()
        except Exception as e:
            sys.exit(f"Could not launch {BROWSER}: {e}\n"
                     "For WebKit set PLAYWRIGHT_BROWSERS_PATH or run: python3 -m playwright install webkit")
        for label, opts in VIEWPORTS.items():
            passed += run_checks(browser, label, opts)
        passed += check_overflow(browser)
        browser.close()
    print(f"\n{passed} checks passed ({BROWSER}).")
    print("All tests passed.")


if __name__ == "__main__":
    main()
