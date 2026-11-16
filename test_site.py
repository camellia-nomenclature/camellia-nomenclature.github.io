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
The checks above stop at the first failure. The item 2 checks (copy link, copy button name,
one render per navigation) run after them; each reports PASS/FAIL and the run exits nonzero
at the end if any failed. The copy-link check only writes the headless browser's own clipboard.
Success ends with "All tests passed."
"""
import os
import re
import sys
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright, expect

BASE = os.environ.get("BASE_URL", "http://localhost:8081").rstrip("/")
BROWSER = os.environ.get("TEST_BROWSER", "chromium")
if BROWSER not in ("chromium", "webkit"):
    sys.exit(f"TEST_BROWSER must be 'chromium' or 'webkit', got {BROWSER!r}")
TIMEOUT = 8000  # ms for card to render
ORIGIN = "{0.scheme}://{0.netloc}".format(urlsplit(BASE))

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


# --- Item 2 checks: copy link (F04), copy button name (F03), one render per navigation (F16) ---

QUOTE_NAME = 'Robert "Bobby" Moore'
QUOTE_HASH = "#Robert%20%22Bobby%22%20Moore"

# Records the text passed to navigator.clipboard.writeText, then calls the real method.
RECORD_CLIPBOARD = """
if (window.Clipboard && Clipboard.prototype.writeText) {
  const write = Clipboard.prototype.writeText;
  Clipboard.prototype.writeText = function (text) { window.__copiedText = text; return write.call(this, text); };
}"""

# Clipboard stubs for the fallback tests (installed before the page's scripts run).
NO_CLIPBOARD_API = "Object.defineProperty(Navigator.prototype, 'clipboard', { get: () => undefined, configurable: true });"
# The first writeText call returns a promise the test settles itself with
# window.__firstWrite.resolve() / REJECT_FIRST_WRITE (deterministic, and the fallback then
# runs after an await). Later calls use the real method.
CONTROL_FIRST_WRITE = """
if (window.Clipboard) {
  const write = Clipboard.prototype.writeText; let calls = 0;
  Clipboard.prototype.writeText = function (text) {
    if (calls++ > 0) return write.call(this, text);
    return new Promise((resolve, reject) => { window.__firstWrite = { resolve, reject }; });
  };
}"""
REJECT_FIRST_WRITE = "window.__firstWrite.reject(new DOMException('denied', 'NotAllowedError'))"
# Counts finished copyLink() calls (installed after the page's script has defined copyLink).
TRACK_COPY_DONE = """() => {
  window.__copyDone = 0;
  const copy = window.copyLink;
  window.copyLink = (...args) => copy(...args).finally(() => { window.__copyDone++; });
}"""
# Records every document.execCommand call: command, return value and the text in the copy
# fallback's textarea. With window.__execResult set, returns that instead of copying.
SPY_EXEC_COMMAND = """
window.__execCalls = [];
const exec = Document.prototype.execCommand;
Document.prototype.execCommand = function (command, ...args) {
  const field = document.querySelector('body > textarea');
  const result = 'execResult' in window.__stub ? window.__stub.execResult : exec.call(this, command, ...args);
  window.__execCalls.push({ command, result, text: field ? field.value : null });
  return result;
};"""
STUB_EXEC_FALSE = "window.__stub = { execResult: false };"
NO_STUB = "window.__stub = {};"

# A "render" is one call into a view renderer, counted by the write that starts the view in
# #results (one childList MutationRecord per innerHTML assignment that changes children):
#   detail view: the "Loading details..." card; list views: a card whose h2 starts with
#   "Species (", "Fragrant Camellias (" or "Yellow Camellias ("; home: #results emptied.
# The later write that replaces "Loading details..." with the full card belongs to the same
# render and is not counted. hashchange is counted too, so the test can wait for it to fire.
# Known blind spot: setting innerHTML = '' on an already-empty #results produces no record,
# so a second home render is invisible and the "Back to home" step cannot catch a double render.
RENDER_COUNTER = """
window.__renders = 0; window.__hashchanges = 0;
addEventListener('hashchange', () => { window.__hashchanges++; });
document.addEventListener('DOMContentLoaded', () => {
  const results = document.getElementById('results');
  new MutationObserver(records => {
    for (const r of records) {
      const card = [...r.addedNodes].find(n => n.nodeType === 1);
      if (card) {
        const h2 = card.querySelector('h2')?.textContent || '';
        if (card.textContent.includes('Loading details...') ||
            /^(Species|Fragrant Camellias|Yellow Camellias) \(/.test(h2)) window.__renders++;
      } else if (r.removedNodes.length && !results.firstElementChild) {
        window.__renders++;
      }
    }
  }).observe(results, { childList: true });
});"""


def test_copy_link(context):
    grant_clipboard(context)
    context.add_init_script(RECORD_CLIPBOARD)
    page, js_errors = make_page(context)
    page.goto(BASE + "/" + QUOTE_HASH, wait_until="commit", timeout=10000)
    expect_card(page, QUOTE_NAME)
    button = page.locator(".card-header button[title='Copy link']")
    button.click()
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)  # the existing ✅ feedback
    assert not js_errors, f"JS errors: {js_errors}"
    if BROWSER == "chromium":
        method = "clipboard read back with navigator.clipboard.readText()"
        copied = page.evaluate("navigator.clipboard.readText()")
    else:
        method = "URL captured by wrapping navigator.clipboard.writeText"
        copied = page.evaluate("window.__copiedText ?? null")
    assert copied, "Nothing was written to the clipboard"
    page_url = page.evaluate("location.origin + location.pathname")
    assert copied.startswith(page_url + "#"), f"Copied {copied!r}, expected a link to {page_url}"
    decoded = page.evaluate("u => decodeURIComponent(new URL(u).hash.slice(1))", copied)
    assert decoded == QUOTE_NAME, f"Copied link decodes to {decoded!r}, expected {QUOTE_NAME!r}"
    return method


def open_quote_card(context, *init_scripts):
    for script in init_scripts:
        context.add_init_script(script)
    page, js_errors = make_page(context)
    page.goto(BASE + "/" + QUOTE_HASH, wait_until="commit", timeout=10000)
    expect_card(page, QUOTE_NAME)
    button = page.locator(".card-header button[title='Copy link']")
    status = page.locator(".card-header [role='status']")
    return page, js_errors, button, status


def assert_fallback_copied(page):
    calls = page.evaluate("window.__execCalls")
    copies = [c for c in calls if c["command"] == "copy"]
    assert copies, f"Fallback did not run: execCommand calls = {calls}"
    assert copies[-1]["result"] is True, f"execCommand('copy') returned {copies[-1]['result']!r}"
    copied = copies[-1]["text"]
    assert copied, "No text in the copy fallback's textarea"
    decoded = page.evaluate("u => decodeURIComponent(new URL(u).hash.slice(1))", copied)
    assert decoded == QUOTE_NAME, f"Copied link decodes to {decoded!r}, expected {QUOTE_NAME!r}"


def test_copy_fallback_no_clipboard_api(context):
    page, js_errors, button, status = open_quote_card(context, NO_STUB, NO_CLIPBOARD_API, SPY_EXEC_COMMAND)
    assert page.evaluate("navigator.clipboard === undefined"), "navigator.clipboard stub not applied"
    button.click()
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    expect(status).to_have_text("Link copied")
    assert_fallback_copied(page)
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_fallback_after_rejected_write(context):
    page, js_errors, button, status = open_quote_card(context, NO_STUB, CONTROL_FIRST_WRITE, SPY_EXEC_COMMAND)
    button.click()
    page.wait_for_function("window.__firstWrite", timeout=TIMEOUT)
    page.evaluate(REJECT_FIRST_WRITE)
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    expect(status).to_have_text("Link copied")
    assert_fallback_copied(page)
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_failure(context):
    page, js_errors, button, status = open_quote_card(context, STUB_EXEC_FALSE, NO_CLIPBOARD_API, SPY_EXEC_COMMAND)
    button.click()
    expect(button).to_have_text("\u274c", timeout=TIMEOUT)  # ❌
    text = status.text_content().strip()  # strip() also drops the VoiceOver \u00A0 suffix
    assert text == "Copy failed", f"Status text {text!r}, expected 'Copy failed'"
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_rejected_and_fallback_fails(context):
    page, js_errors, button, status = open_quote_card(context, STUB_EXEC_FALSE, CONTROL_FIRST_WRITE, SPY_EXEC_COMMAND)
    page.evaluate(TRACK_COPY_DONE)
    button.click()
    page.wait_for_function("window.__firstWrite", timeout=TIMEOUT)
    page.evaluate(REJECT_FIRST_WRITE)
    page.wait_for_function("window.__copyDone >= 1", timeout=TIMEOUT)
    calls = [c for c in page.evaluate("window.__execCalls") if c["command"] == "copy"]
    assert calls and calls[-1]["result"] is False, f"Fallback not invoked as expected: {calls}"
    expect(button).to_have_text("\u274c")  # ❌
    text = status.text_content().strip()
    assert text == "Copy failed", f"Status text {text!r}, expected 'Copy failed'"
    assert page.locator("body > textarea").count() == 0, "Fallback textarea was not removed"
    expect(button).to_have_text("\U0001f517", timeout=3000)  # reset to 🔗
    expect(status).to_be_empty(timeout=3000)
    assert not js_errors, f"JS errors: {js_errors}"


def grant_clipboard(context):
    if BROWSER == "chromium":
        context.grant_permissions(["clipboard-read", "clipboard-write"], origin=ORIGIN)


def test_copy_status_text(context):
    grant_clipboard(context)
    page, js_errors, button, status = open_quote_card(context)
    button.click()
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    expect(status).to_have_text("Link copied")
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_feedback_resets(context):
    grant_clipboard(context)
    page, js_errors, button, status = open_quote_card(context)
    button.click()
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    expect(button).to_have_text("\U0001f517", timeout=3000)  # back to 🔗 (reset after 1.2 s)
    expect(status).to_be_empty(timeout=3000)
    assert not js_errors, f"JS errors: {js_errors}"


def copy_on_a_then_open_b(context, *init_scripts):
    """Card A (Yuletide): click copy, its first writeText stays pending. Then open card B."""
    grant_clipboard(context)
    for script in (CONTROL_FIRST_WRITE,) + init_scripts:
        context.add_init_script(script)
    page, js_errors = make_page(context)
    page.goto(f"{BASE}/#Yuletide", wait_until="commit", timeout=10000)
    expect_card(page, "Yuletide")
    page.evaluate(TRACK_COPY_DONE)
    page.locator(".card-header button[title='Copy link']").click()
    page.wait_for_function("window.__firstWrite", timeout=TIMEOUT)
    page.fill("#search", "Kwan Yuen Special")
    page.locator(".dropdown-item").first.click()
    expect_card(page, "Kwan Yuen Special")
    return page, js_errors, page.locator(".card-header button[title='Copy link']")


def test_copy_timer_per_button(context):
    """A late copy on an old card must not cancel a newer card's reset (one shared timer did)."""
    page, js_errors, button = copy_on_a_then_open_b(context)
    button.click()  # card B
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    page.evaluate("window.__firstWrite.resolve()")  # card A's copy finishes before B's reset
    page.wait_for_function("window.__copyDone >= 2", timeout=TIMEOUT)  # A's copyLink has run
    expect(button).to_have_text("\U0001f517", timeout=3000)
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_stale_card(context):
    """Card A's write rejects after B replaced it: no fallback, no feedback, B unaffected."""
    page, js_errors, button = copy_on_a_then_open_b(context, NO_STUB, SPY_EXEC_COMMAND)
    status = page.locator(".card-header [role='status']")
    focused = page.evaluate("document.activeElement && document.activeElement.outerHTML.slice(0, 60)")
    page.evaluate(REJECT_FIRST_WRITE)
    page.wait_for_function("window.__copyDone >= 1", timeout=TIMEOUT)  # A's copyLink has run
    calls = [c for c in page.evaluate("window.__execCalls") if c["command"] == "copy"]
    assert not calls, f"Fallback ran for a replaced card: {calls}"
    expect(button).to_have_text("\U0001f517")
    expect(status).to_be_empty()
    now = page.evaluate("document.activeElement && document.activeElement.outerHTML.slice(0, 60)")
    assert now == focused, f"Focus moved from {focused!r} to {now!r}"
    button.click()  # card B still copies normally
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_button_name(context):
    page, js_errors = make_page(context)
    page.goto(f"{BASE}/#Kwan%20Yuen%20Special", wait_until="commit", timeout=10000)
    expect_card(page, "Kwan Yuen Special")
    expect(page.locator(".card-header").get_by_role("button", name="Copy link", exact=True)).to_have_count(1)
    assert not js_errors, f"JS errors: {js_errors}"


def test_render_count(context):
    context.add_init_script(RENDER_COUNTER)
    page, js_errors = make_page(context)
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_startup(page)

    def species():
        expect(page.locator(".card-header h2")).to_contain_text("Species (", timeout=TIMEOUT)

    def home():
        expect(page.locator("#homeSection")).to_be_visible(timeout=TIMEOUT)
        expect(page.locator("#results .card")).to_have_count(0, timeout=TIMEOUT)

    def search_select():
        page.fill("#search", "Kwan Yuen Special")
        page.locator(".dropdown-item").first.click()

    steps = [  # (navigation, action, wait for the resulting view, fires hashchange?)
        ("home pill #species", lambda: page.click("a[href='#species']"), species, True),
        ("Back to home", page.go_back, home, True),
        ("Forward to #species", page.go_forward, species, True),
        ("location.hash = #Yuletide", lambda: page.evaluate("location.hash = '#Yuletide'"),
         lambda: expect_card(page, "Yuletide"), True),
        ("search-select (pushState)", search_select, lambda: expect_card(page, "Kwan Yuen Special"), False),
        ("Back between two details", page.go_back, lambda: expect_card(page, "Yuletide"), True),
    ]
    counts = {}
    for name, action, wait_view, fires_hashchange in steps:
        renders, hashchanges = page.evaluate("[window.__renders, window.__hashchanges]")
        action()
        if fires_hashchange:
            # popstate is handled before hashchange; once hashchange has fired, every listener
            # for this navigation has run, so the count below is final.
            page.wait_for_function("n => window.__hashchanges > n", arg=hashchanges, timeout=TIMEOUT)
        wait_view()
        counts[name] = page.evaluate("window.__renders") - renders
    assert all(n == 1 for n in counts.values()), f"Renders per navigation (want 1 each): {counts}"
    assert not js_errors, f"JS errors: {js_errors}"


ITEM2_TESTS = (
    ('copy link for Robert "Bobby" Moore', test_copy_link),
    ("copy button accessible name", test_copy_button_name),
    ("one render per navigation", test_render_count),
    ("copy fallback without Clipboard API (execCommand)", test_copy_fallback_no_clipboard_api),
    ("copy fallback after writeText rejects", test_copy_fallback_after_rejected_write),
    ("copy failure shows the failed state", test_copy_failure),
    ('copy status reads "Link copied"', test_copy_status_text),
    ("copy feedback resets to the link icon", test_copy_feedback_resets),
    ("copy feedback timer is per button", test_copy_timer_per_button),
    ("copy fallback returns false after writeText rejects", test_copy_rejected_and_fallback_fails),
    ("late rejection on a replaced card is ignored", test_copy_stale_card),
)


# --- Item 3 checks: search combobox (F01), Esc/Tab (F10), link names (F13), back button (F03) ---

def width_opts(width):
    opts = dict(viewport={"width": width, "height": 900 if width > 600 else 844})
    if width <= 600:
        opts.update(is_mobile=True, has_touch=True)
    return opts


def open_search(context, query):
    page, js_errors = make_page(context)
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_data(page)
    page.click("#search")
    page.keyboard.type(query, delay=20)
    expect(page.locator("#dropdown")).to_be_visible(timeout=TIMEOUT)
    return page, js_errors


ACTIVE_STATE = """() => {
  const s = document.getElementById('search');
  return {
    focused: document.activeElement === s,
    expanded: s.getAttribute('aria-expanded'),
    active: s.getAttribute('aria-activedescendant'),
    ids: [...document.querySelectorAll('#dropdown [role=option]')].map(o => o.id),
    selected: [...document.querySelectorAll('#dropdown [aria-selected=true]')].map(o => o.id),
  };
}"""


def test_search_names(context):
    page, js_errors = open_search(context, "kwan")
    expect(page.get_by_role("combobox", name="Camellia name", exact=True)).to_have_count(1)
    listbox = page.get_by_role("listbox", name="Matching camellias", exact=True)
    expect(listbox).to_be_visible()
    options = listbox.get_by_role("option")
    n = options.count()
    assert n > 0, "No options in the listbox"
    expect(listbox.get_by_role("option", name=re.compile(r"\S"))).to_have_count(n)  # all named
    page.goto(f"{BASE}/#Kwan%20Yuen%20Special", wait_until="commit", timeout=10000)
    expect_card(page, "Kwan Yuen Special")
    expect(page.locator(".card-header").get_by_role("button", name="Go back", exact=True)).to_have_count(1)
    assert not js_errors, f"JS errors: {js_errors}"


def test_home_link_names(context):
    page, js_errors = make_page(context)
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_data(page)
    for text in ("Browse species", "Fragrant Camellias", "Yellow Camellias"):
        expect(page.get_by_role("link", name=text, exact=True)).to_have_count(1)
    assert not js_errors, f"JS errors: {js_errors}"


def test_arrow_keys(context):
    page, js_errors = open_search(context, "kwan")
    seen = []
    for key, want in (("ArrowDown", 0), ("ArrowDown", 1), ("ArrowUp", 0)):
        page.keyboard.press(key)
        st = page.evaluate(ACTIVE_STATE)
        seen.append(st)
        assert len(st["ids"]) > 1 and len(set(st["ids"])) == len(st["ids"]) and all(st["ids"]), f"Option ids: {st['ids']}"
        want_id = st["ids"][want]
        assert st["expanded"] == "true", f"aria-expanded={st['expanded']!r} while open"
        assert st["active"] == want_id, f"{key}: aria-activedescendant={st['active']!r}, want {want_id!r}"
        assert st["selected"] == [want_id], f"{key}: aria-selected=true on {st['selected']}, want [{want_id!r}]"
        assert st["focused"], f"{key}: focus left #search"
    assert not js_errors, f"JS errors: {js_errors}"


def test_escape_and_tab(context):
    page, js_errors = open_search(context, "kwan")
    dropdown = page.locator("#dropdown")
    search = page.locator("#search")
    expect(search).to_have_attribute("aria-expanded", "true")
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Escape")
    expect(search).to_have_attribute("aria-expanded", "false")
    expect(dropdown).to_be_hidden()
    st = page.evaluate(ACTIVE_STATE)
    assert st["focused"], "Escape moved focus out of #search"
    assert st["active"] is None, f"aria-activedescendant left set: {st['active']!r}"
    assert search.input_value() == "kwan", f"Escape changed the text to {search.input_value()!r}"
    page.keyboard.press("Backspace")  # "kwa": reopen the list
    expect(dropdown).to_be_visible(timeout=TIMEOUT)
    page.keyboard.press("Tab")
    expect(search).to_have_attribute("aria-expanded", "false")
    expect(dropdown).to_be_hidden()
    assert not js_errors, f"JS errors: {js_errors}"


def test_match_count(context):
    page, js_errors = open_search(context, "kwan")
    status = page.locator("#searchStatus")
    n = page.locator("#dropdown [role=option]").count()
    assert n > 0, "No [role=option] items in the list"
    expect(status).to_have_text(f"{n} matches" if n != 1 else "1 match", timeout=3000)
    page.fill("#search", "zzqxjv")
    expect(status).to_have_text("No matches", timeout=3000)
    page.fill("#search", "a")
    expect(status).to_have_text(re.compile(r"^\d+ matches, showing first 20$"), timeout=3000)
    total = int(status.text_content().split()[0])
    shown = page.locator("#dropdown [role=option]").count()
    assert total > shown == 20, f"Status {status.text_content()!r} but {shown} options shown"
    assert not js_errors, f"JS errors: {js_errors}"


SEARCH_FOCUSED = "document.activeElement === document.getElementById('search')"

FOCUS_INFO = """() => {
  const a = document.activeElement, r = a.getBoundingClientRect();
  return { tag: a.tagName, id: a.id, text: (a.textContent || '').trim(),
           heading: a.matches('.card-header h2'), href: a.getAttribute('href'),
           camellia: a.dataset ? a.dataset.camellia || null : null,
           inView: r.bottom > 0 && r.top < innerHeight, scrollY: Math.round(scrollY) };
}"""


def assert_heading_focused(page, text, prefix=False):
    """Focus is on the current view's heading (.card-header h2) and it is on screen."""
    f = page.evaluate(FOCUS_INFO)
    ok = f["heading"] and (f["text"].startswith(text) if prefix else f["text"] == text)
    assert ok, f"Expected focus on heading {text!r}, got {f['tag']} {f['text'][:40]!r}"
    assert f["inView"], f"Focused heading {text!r} is off-screen (scrollY={f['scrollY']})"
    return f


def test_option_pick(context):
    """Tapping (touch) or clicking an option selects it and leaves the search box
    (so a phone's on-screen keyboard drops)."""
    page, js_errors = open_search(context, "Kwan Yuen Special")
    option = page.locator(".dropdown-item", has_text="Kwan Yuen Special").first  # behaviour, not roles
    if page.evaluate("matchMedia('(pointer: coarse)').matches"):
        option.tap()
    else:
        option.click()
    expect_card(page, "Kwan Yuen Special")
    expect(page.locator("#search")).to_have_attribute("aria-expanded", "false")
    assert not page.evaluate(SEARCH_FOCUSED), "Focus stayed in #search after picking an option"
    assert_heading_focused(page, "Kwan Yuen Special")
    assert not js_errors, f"JS errors: {js_errors}"


def test_enter_pick(context):
    page, js_errors = open_search(context, "Kwan Yuen Special")
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Enter")
    expect_card(page, "Kwan Yuen Special")
    assert not page.evaluate(SEARCH_FOCUSED), "Focus stayed in #search after Enter on an option"
    assert_heading_focused(page, "Kwan Yuen Special")
    assert not js_errors, f"JS errors: {js_errors}"


def assert_list_open(page, is_open):
    """The list's visibility, plus aria-expanded whenever #search is a combobox
    (so the same check also runs on the pre-combobox cc3108f baseline)."""
    dropdown = page.locator("#dropdown")
    expect(dropdown).to_be_visible() if is_open else expect(dropdown).to_be_hidden()
    if page.locator("#search").get_attribute("role") == "combobox":
        expect(page.locator("#search")).to_have_attribute("aria-expanded", "true" if is_open else "false")


def test_blur_keeps_list(context):
    """A bare blur (e.g. iOS keyboard "Done") leaves the list as it was, like cc3108f."""
    page, js_errors = open_search(context, "kwan")
    page.evaluate("document.activeElement.blur()")
    assert_list_open(page, True)
    assert page.locator("#search").input_value() == "kwan"
    assert not js_errors, f"JS errors: {js_errors}"


def test_outside_click(context):
    """Clicks inside .search-container keep the list; a click outside closes it, like cc3108f."""
    page, js_errors = open_search(context, "kwan")
    page.locator("#search").click()
    assert_list_open(page, True)
    page.locator(".header").click(position={"x": 5, "y": 5})  # header background, not its link
    assert_list_open(page, False)
    assert not js_errors, f"JS errors: {js_errors}"


def test_caret_keys_clear_option(context):
    """Home / ArrowLeft after ArrowDown return to text editing: no active option, and Enter
    then does nothing (as cc3108f does with no selection)."""
    page, js_errors = open_search(context, "kwan")
    for key in ("Home", "ArrowLeft"):
        page.keyboard.press("ArrowDown")
        st = page.evaluate(ACTIVE_STATE)
        assert st["active"], f"ArrowDown did not set an active option before {key}"
        page.keyboard.press(key)
        st = page.evaluate(ACTIVE_STATE)
        assert st["active"] is None, f"{key}: aria-activedescendant still {st['active']!r}"
        assert st["selected"] == [], f"{key}: options still aria-selected: {st['selected']}"
        assert st["expanded"] == "true", f"{key}: list closed (aria-expanded={st['expanded']!r})"
        assert st["focused"], f"{key}: focus left #search"
    page.keyboard.press("Enter")
    assert page.locator("#results .card").count() == 0, "Enter opened a card with no active option"
    assert page.evaluate("location.hash") == "", f"Enter changed the hash to {page.url}"
    assert_list_open(page, True)
    assert page.locator("#search").input_value() == "kwan"
    assert not js_errors, f"JS errors: {js_errors}"


def test_match_count_written_once(context):
    """An unchanged count is not rewritten (no repeat announcement). The page clock is
    controlled so the 400 ms debounce fires exactly when the test says."""
    page, js_errors = make_page(context)
    page.clock.install()
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_data(page)
    status = page.locator("#searchStatus")
    page.click("#search")
    page.keyboard.type("kwan")
    page.clock.run_for(500)
    expect(status).to_have_text("3 matches")
    page.evaluate("""() => { window.__statusWrites = 0;
      new MutationObserver(r => { window.__statusWrites += r.length; })
        .observe(document.getElementById('searchStatus'), { childList: true, characterData: true, subtree: true }); }""")
    page.keyboard.type(" ")  # "kwan " searches the same thing: still 3 matches
    page.clock.run_for(500)
    writes_same = page.evaluate("window.__statusWrites")
    page.keyboard.type("y")  # "kwan y": 2 matches
    page.clock.run_for(500)
    expect(status).to_have_text("2 matches")
    writes_total = page.evaluate("window.__statusWrites")
    assert writes_same == 0, f"Unchanged count was rewritten ({writes_same} mutation records)"
    assert writes_total > 0, "Changed count was not written"
    assert not js_errors, f"JS errors: {js_errors}"


def test_copy_focus_return(context):
    page, js_errors, button, status = open_quote_card(context, NO_STUB, NO_CLIPBOARD_API, SPY_EXEC_COMMAND)
    button.click()
    expect(button).to_have_text("\u2705", timeout=TIMEOUT)
    assert_fallback_copied(page)
    focused = page.evaluate("document.activeElement && document.activeElement.className")
    assert focused == "copy-link-btn", f"Focus after fallback copy is on {focused!r}, not the copy button"
    assert not js_errors, f"JS errors: {js_errors}"


def test_open_list_overflow(context):
    page, js_errors = open_search(context, "a")  # common query: full 20-item list
    url = page.url
    width = page.viewport_size["width"]
    m = page.evaluate("""() => ({
        html_sw: document.documentElement.scrollWidth, html_cw: document.documentElement.clientWidth,
        body_sw: document.body.scrollWidth})""")
    assert m["html_sw"] <= m["html_cw"], (
        f"Horizontal overflow with the list open on {url} at width {width}: html scrollWidth={m['html_sw']} > clientWidth={m['html_cw']}")
    assert m["body_sw"] <= m["html_cw"], (
        f"Horizontal overflow with the list open on {url} at width {width}: body scrollWidth={m['body_sw']} > clientWidth={m['html_cw']}")
    assert not js_errors, f"JS errors: {js_errors}"


# --- Item 4 checks: focus on view changes / Back (F02), titles (F11), ✕ on deep link (F15) ---

HOME_TITLE = "Camellia Lookup"


def open_home(context):
    page, js_errors = make_page(context)
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_startup(page)
    return page, js_errors


def expect_list(page, prefix):
    expect(page.locator(".card-header h2")).to_contain_text(prefix, timeout=TIMEOUT)


def expect_home(page):
    expect(page.locator("#homeSection")).to_be_visible(timeout=TIMEOUT)
    expect(page.locator("#results .card")).to_have_count(0, timeout=TIMEOUT)


def focus_info(page):
    return page.evaluate(FOCUS_INFO)


def test_focus_pills_and_hash(context):
    """Pill → list heading (scrollY stays 0, as before); Back → that pill; hash edit → card heading."""
    page, js_errors = open_home(context)
    for href, prefix in (("#species", "Species ("), ("#fragrant", "Fragrant Camellias (")):
        page.click(f"a.home-link[href='{href}']")
        expect_list(page, prefix)
        f = assert_heading_focused(page, prefix, prefix=True)
        assert f["scrollY"] == 0, f"Pill {href}: scrollY {f['scrollY']}, expected 0 (unchanged behaviour)"
        page.go_back()
        expect_home(page)
        f = focus_info(page)
        assert f["href"] == href, f"Back to home: focus on {f['tag']} {f['href'] or f['id']!r}, expected the {href} pill"
    page.evaluate("location.hash = '#Yuletide'")  # same-document hash navigation
    expect_card(page, "Yuletide")
    f = assert_heading_focused(page, "Yuletide")
    assert f["scrollY"] == 0, f"Hash navigation: scrollY {f['scrollY']}, expected 0"
    assert not js_errors, f"JS errors: {js_errors}"


def test_focus_back_forward(context):
    page, js_errors = open_home(context)
    page.click("a.home-link[href='#species']")
    expect_list(page, "Species (")
    page.evaluate("location.hash = '#Yuletide'")
    expect_card(page, "Yuletide")
    assert_heading_focused(page, "Yuletide")
    page.go_back()
    expect_list(page, "Species (")
    assert_heading_focused(page, "Species (", prefix=True)
    page.go_forward()
    expect_card(page, "Yuletide")
    assert_heading_focused(page, "Yuletide")
    assert not js_errors, f"JS errors: {js_errors}"


def test_focus_back_to_list_keeps_scroll(context):
    """Far list row → card (heading focused, scrollY 0 as before) → Back: scroll restored and
    the activated list link focused; Forward: card heading again."""
    page, js_errors = open_home(context)
    page.click("a.home-link[href='#fragrant']")
    expect_list(page, "Fragrant Camellias (")
    link = page.locator(".list-link").nth(120)
    link.scroll_into_view_if_needed()
    y0 = page.evaluate("Math.round(scrollY)")
    name = link.get_attribute("data-camellia")
    link.click()
    expect_card(page, name)
    f = assert_heading_focused(page, name)
    assert f["scrollY"] == 0, f"List link: scrollY {f['scrollY']}, expected 0 (unchanged behaviour)"
    page.go_back()
    expect_list(page, "Fragrant Camellias (")
    page.wait_for_function("y0 => Math.abs(scrollY - y0) <= 2", arg=y0, timeout=TIMEOUT)
    f = focus_info(page)
    assert f["camellia"] == name, f"Back to list: focus on {f['tag']} {f['text'][:40]!r}, expected link {name!r}"
    assert f["inView"], "Restored list link is off-screen"
    page.go_forward()
    expect_card(page, name)
    assert_heading_focused(page, name)
    assert not js_errors, f"JS errors: {js_errors}"


HOME_HEADING_FOCUSED = "document.activeElement === document.querySelector('.header h1')"


def test_focus_back_to_home_fallback(context):
    """Back to home: the pill that opened the view (pills test); otherwise (search pick, hash
    edit, remembered element gone) the home h1. #search is never focused by navigation."""
    page, js_errors = open_home(context)
    page.fill("#search", "Kwan Yuen Special")  # search pick: nothing is remembered
    page.locator(".dropdown-item").first.click()
    expect_card(page, "Kwan Yuen Special")
    page.go_back()
    expect_home(page)
    f = focus_info(page)
    assert page.evaluate(HOME_HEADING_FOCUSED), f"Search pick + Back: focus on {f}"
    assert f["scrollY"] == 0, f"Search pick + Back: scrollY {f['scrollY']}, expected 0"
    page.evaluate("location.hash = '#Yuletide'")  # hash edit: nothing remembered
    expect_card(page, "Yuletide")
    page.go_back()
    expect_home(page)
    assert page.evaluate(HOME_HEADING_FOCUSED), f"Hash edit + Back: focus on {focus_info(page)}"
    # opener gone: the remembered element no longer exists
    page.evaluate("""() => history.replaceState({ ...history.state, returnFocus: 'a.home-link[href="#gone"]' }, '')""")
    page.evaluate("location.hash = '#Yuletide'")
    expect_card(page, "Yuletide")
    page.go_back()
    expect_home(page)
    f = focus_info(page)
    assert page.evaluate(HOME_HEADING_FOCUSED), f"Missing opener + Back: focus on {f}"
    assert f["scrollY"] == 0, f"Missing opener + Back: scrollY {f['scrollY']}, expected 0"
    assert not page.evaluate(SEARCH_FOCUSED)
    assert not js_errors, f"JS errors: {js_errors}"


def test_focus_back_between_picks(context):
    """Search pick A, then search pick B from A's card, then Back: card A's heading."""
    page, js_errors = open_home(context)
    for name in ("Yuletide", "Kwan Yuen Special"):
        page.fill("#search", name)
        page.locator(".dropdown-item", has_text=name).first.click()
        expect_card(page, name)
    page.go_back()
    expect_card(page, "Yuletide")
    assert_heading_focused(page, "Yuletide")
    assert not js_errors, f"JS errors: {js_errors}"


def test_stale_card_load(context):
    """A card whose data arrives after the user moved on neither replaces the newer view nor
    takes focus (Y.json is held until the species list is open)."""
    page, js_errors = open_home(context)
    held = []
    page.route("**/data/Y.json", lambda route: held.append(route))
    page.evaluate("location.hash = '#Yuletide'")  # starts loading, data held
    expect(page.locator(".card-body")).to_contain_text("Loading details...", timeout=TIMEOUT)
    for _ in range(200):  # wait for the request to reach the route handler
        if held:
            break
        page.wait_for_timeout(25)
    assert held, "Y.json request was not intercepted"
    page.evaluate("location.hash = '#species'")  # the user moves on
    expect_list(page, "Species (")
    assert_heading_focused(page, "Species (", prefix=True)
    held[0].continue_()  # Yuletide's data now arrives
    page.wait_for_function("typeof entryCache.Y === 'object'", timeout=TIMEOUT)
    page.evaluate("() => new Promise(r => setTimeout(r, 0))")  # let the old load finish
    expect(page.locator(".card-header h2")).to_contain_text("Species (")
    assert_heading_focused(page, "Species (", prefix=True)
    expect(page).to_have_title(f"Species | {HOME_TITLE}")
    assert not js_errors, f"JS errors: {js_errors}"


RING = """(sel) => {
  const el = document.querySelector(sel), cs = getComputedStyle(el);
  return { focused: document.activeElement === el, focusVisible: el.matches(':focus-visible'),
           style: cs.outlineStyle, width: parseFloat(cs.outlineWidth), color: cs.outlineColor,
           bg: getComputedStyle(el.closest('.card-header, .header')).backgroundImage };
}"""


def _rgb_list(text):
    return [tuple(int(float(v)) for v in m[:3]) for m in re.findall(r"rgba?\(([\d.]+),\s*([\d.]+),\s*([\d.]+)", text)]


def _contrast(a, b):
    def lum(c):
        ch = [v / 255 for v in c]
        ch = [v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4 for v in ch]
        return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2]
    hi, lo = sorted((lum(a), lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def assert_ring(page, sel):
    r = page.evaluate(RING, sel)
    assert r["focused"], f"{sel} is not focused"
    assert r["focusVisible"], f"{sel} does not match :focus-visible after a keyboard action"
    assert r["style"] != "none" and r["width"] >= 2, f"{sel} focus ring: {r['style']} {r['width']}px"
    ring, stops = _rgb_list(r["color"])[0], _rgb_list(r["bg"])
    assert stops, f"No header background colours in {r['bg']!r}"
    worst = min(_contrast(ring, s) for s in stops)
    assert worst >= 3, f"{sel} ring {r['color']} contrast {worst:.2f}:1 vs header {stops} (< 3:1)"
    return worst


# WebKit only applies :focus-visible to script focus made synchronously inside a keydown that
# is not default-prevented; the app focuses after a fetch or popstate, so in WebKit the ring is
# exercised by re-focusing the heading from such a keydown (test-only listener, F2).
REFOCUS_ON_F2 = """(sel) => document.addEventListener('keydown', e => {
  if (e.key === 'F2') document.querySelector(sel).focus({ preventScroll: true });
}, { once: true })"""


def check_ring(page, sel):
    if BROWSER == "webkit":
        page.evaluate(REFOCUS_ON_F2, sel)
        page.keyboard.press("F2")
    return assert_ring(page, sel)


def test_heading_focus_ring(context):
    """Keyboard path: Enter on an option focuses the card heading; Enter on the focused back
    button returns home and focuses the h1. Both get a visible :focus-visible ring, >=3:1 against
    the header gradient's stops."""
    page, js_errors = open_search(context, "Kwan Yuen Special")
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Enter")
    expect_card(page, "Kwan Yuen Special")
    assert_heading_focused(page, "Kwan Yuen Special")
    card = check_ring(page, ".card-header h2")
    page.locator(".card-header button[title='Go back']").focus()
    page.keyboard.press("Enter")
    expect_home(page)
    assert page.evaluate(HOME_HEADING_FOCUSED), f"Back to home: focus on {focus_info(page)}"
    home = check_ring(page, ".header h1")
    assert not js_errors, f"JS errors: {js_errors}"
    how = "app focus path" if BROWSER != "webkit" else "app focus, ring via F2 keydown re-focus"
    return f"{how}; min ring contrast: card {card:.2f}:1, home {home:.2f}:1"


def test_initial_deep_link_keeps_focus(context):
    """Decision: the initial page load never moves focus or scrolls (html guide: focus moves
    are for user-initiated view changes)."""
    for path, wait in (("/#Kwan%20Yuen%20Special", lambda p: expect_card(p, "Kwan Yuen Special")),
                       ("/#fragrant", lambda p: expect_list(p, "Fragrant Camellias ("))):
        page, js_errors = make_page(context)
        page.goto(BASE + path, wait_until="commit", timeout=10000)
        wait(page)
        wait_for_startup(page)
        f = focus_info(page)
        assert f["tag"] == "BODY", f"{path}: initial load moved focus to {f['tag']} {f['text'][:40]!r}"
        assert f["scrollY"] == 0, f"{path}: initial load scrolled to {f['scrollY']}"
        assert not js_errors, f"JS errors on {path}: {js_errors}"
        page.close()


def test_titles(context):
    page, js_errors = open_home(context)
    expect(page).to_have_title(HOME_TITLE)
    for href, title in (("#species", "Species"), ("#fragrant", "Fragrant Camellias"),
                        ("#yellow", "Yellow Camellias")):
        page.click(f"a.home-link[href='{href}']")
        expect(page).to_have_title(f"{title} | {HOME_TITLE}", timeout=TIMEOUT)
        page.go_back()
        expect(page).to_have_title(HOME_TITLE, timeout=TIMEOUT)
    page.fill("#search", "Kwan Yuen Special")
    page.locator(".dropdown-item").first.click()
    expect_card(page, "Kwan Yuen Special")
    expect(page).to_have_title(f"Kwan Yuen Special | {HOME_TITLE}")
    page.go_back()
    expect(page).to_have_title(HOME_TITLE, timeout=TIMEOUT)
    page.goto(f"{BASE}/#Yuletide", wait_until="commit", timeout=10000)  # deep link
    expect_card(page, "Yuletide")
    expect(page).to_have_title(f"Yuletide | {HOME_TITLE}")
    assert not js_errors, f"JS errors: {js_errors}"


def test_clear_button(context):
    """✕ is visible when a card opens from a deep link, hidden on home (and after Back)."""
    page, js_errors = make_page(context)
    page.goto(f"{BASE}/#Kwan%20Yuen%20Special", wait_until="commit", timeout=10000)
    expect_card(page, "Kwan Yuen Special")
    expect(page.locator("#clearBtn")).to_be_visible()
    page2, js_errors2 = open_home(context)
    expect(page2.locator("#clearBtn")).to_be_hidden()
    page2.evaluate("location.hash = '#Yuletide'")
    expect_card(page2, "Yuletide")
    expect(page2.locator("#clearBtn")).to_be_visible()
    page2.go_back()
    expect_home(page2)
    expect(page2.locator("#clearBtn")).to_be_hidden()
    assert not js_errors and not js_errors2, f"JS errors: {js_errors + js_errors2}"


def test_zero_count_not_rewritten(context):
    """0 → 0: once "No matches" is announced, another non-matching query writes nothing."""
    page, js_errors = make_page(context)
    page.clock.install()
    page.goto(BASE + "/", wait_until="commit", timeout=10000)
    wait_for_data(page)
    status = page.locator("#searchStatus")
    page.click("#search")
    page.keyboard.type("zzq")
    page.clock.run_for(500)
    expect(status).to_have_text("No matches")
    page.evaluate("""() => { window.__statusWrites = 0;
      new MutationObserver(r => { window.__statusWrites += r.length; })
        .observe(document.getElementById('searchStatus'), { childList: true, characterData: true, subtree: true }); }""")
    page.keyboard.type("x")  # "zzqx": still no matches
    page.clock.run_for(500)
    writes = page.evaluate("window.__statusWrites")
    assert writes == 0, f"Unchanged 'No matches' was rewritten ({writes} mutation records)"
    expect(status).to_have_text("No matches")
    assert not js_errors, f"JS errors: {js_errors}"


NO_SELECTED_OPTION = "document.querySelectorAll('#dropdown [aria-selected=true]').length"


def test_aria_selected_cleared_on_close(context):
    page, js_errors = open_search(context, "kwan")
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Escape")
    assert page.evaluate(NO_SELECTED_OPTION) == 0, "aria-selected=true left on an option after Escape"
    page.keyboard.press("Backspace")  # "kwa": reopen
    expect(page.locator("#dropdown")).to_be_visible(timeout=TIMEOUT)
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Tab")
    assert page.evaluate(NO_SELECTED_OPTION) == 0, "aria-selected=true left on an option after Tab"
    page.locator("#search").fill("Kwan Yuen Special")
    page.locator("#search").focus()
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Enter")
    expect_card(page, "Kwan Yuen Special")
    assert page.evaluate(NO_SELECTED_OPTION) == 0, "aria-selected=true left on an option after Enter"
    assert not js_errors, f"JS errors: {js_errors}"


ITEM4_TESTS = (
    ("focus: pill → list heading, Back → pill, hash nav → card heading", test_focus_pills_and_hash),
    ("focus: Back/Forward → heading of the restored view", test_focus_back_forward),
    ("focus: Back to a list → activated link, scroll restored", test_focus_back_to_list_keeps_scroll),
    ("focus: Back to home → opener (pill), else the home h1", test_focus_back_to_home_fallback),
    ("focus: search pick A → pick B → Back → card A heading", test_focus_back_between_picks),
    ("slow card load does not replace the newer view or take focus", test_stale_card_load),
    ("header headings get a visible ≥3:1 :focus-visible ring", test_heading_focus_ring),
    ("initial deep link does not move focus or scroll", test_initial_deep_link_keeps_focus),
    ("document.title per view and after Back", test_titles),
    ("clear button visible on deep-link card, hidden on home", test_clear_button),
    ("0 → 0 match count is not rewritten", test_zero_count_not_rewritten),
    ("aria-selected cleared when the list closes (Esc/Tab/Enter)", test_aria_selected_cleared_on_close),
)


ITEM3_TESTS = (
    ("search combobox, listbox, options and back button are named", test_search_names),
    ("home links' names contain their visible text", test_home_link_names),
    ("arrow keys move the active option", test_arrow_keys),
    ("Escape and Tab close the list", test_escape_and_tab),
    ("live match count", test_match_count),
    ("tap/click on an option selects it and leaves the search box", test_option_pick),
    ("Enter on an option selects it and leaves the search box", test_enter_pick),
    ("blur keeps the list open (cc3108f parity)", test_blur_keeps_list),
    ("outside click closes the list, inside click doesn't (cc3108f parity)", test_outside_click),
    ("Home/ArrowLeft clear the active option; Enter then does nothing", test_caret_keys_clear_option),
    ("unchanged match count is not rewritten", test_match_count_written_once),
    ("focus returns to the copy button after a fallback copy", test_copy_focus_return),
)


def run_block(browser, title, cases):
    """Runs (label, context options, name, test) cases; each reports PASS/FAIL."""
    print(title)
    passed, failed = 0, []
    for label, opts, name, test in cases:
        context = browser.new_context(**opts)
        try:
            note = test(context)
            print(f"  PASS: [{label}] {name}" + (f" ({note})" if note else ""))
            passed += 1
        except Exception as e:
            lines = [ln.strip() for ln in str(e).splitlines() if ln.strip()]
            print(f"  FAIL: [{label}] {name}: {' / '.join(lines[:2]) or type(e).__name__}")
            failed.append(f"[{label}] {name}")
        finally:
            context.close()
    return passed, failed


def per_viewport(tests):
    return [(label, opts, name, test) for label, opts in VIEWPORTS.items() for name, test in tests]


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
        p2, f2 = run_block(browser, "Item 2 checks: each reports PASS/FAIL", per_viewport(ITEM2_TESTS))
        p3, f3 = run_block(browser, "Item 3 checks: each reports PASS/FAIL", per_viewport(ITEM3_TESTS) + [
            (f"width {w}", width_opts(w), "no horizontal overflow with the list open", test_open_list_overflow)
            for w in OVERFLOW_WIDTHS])
        p4, f4 = run_block(browser, "Item 4 checks: each reports PASS/FAIL", per_viewport(ITEM4_TESTS))
        browser.close()
    passed += p2 + p3 + p4
    failed = f2 + f3 + f4
    print(f"\n{passed} checks passed ({BROWSER}).")
    if failed:
        print(f"{len(failed)} check(s) FAILED: " + "; ".join(failed))
        sys.exit(1)
    print("All tests passed.")


if __name__ == "__main__":
    main()
