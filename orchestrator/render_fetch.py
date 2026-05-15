import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional


class RenderFetchError(RuntimeError):
    """Raised when Selenium cannot render a page."""


@dataclass
class RenderedResponse:
    """Small adapter for scrapers that expect a requests-like object."""

    url: str
    text: str
    status_code: int = 200

    @property
    def content(self) -> bytes:
        return self.text.encode("utf-8")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RenderFetchError(f"Rendered response failed with status {self.status_code}")


def _make_driver(
    *,
    headless: bool = True,
    user_agent: Optional[str] = None,
    window_size: str = "1400,900",
) -> object:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    chrome_binary = os.environ.get("CHROME_BINARY")
    if chrome_binary:
        opts.binary_location = chrome_binary

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--window-size={window_size}")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")

    return webdriver.Chrome(options=opts)


def fetch_rendered_html(
    url: str,
    *,
    wait_css: Optional[str] = None,
    headless: bool = True,
    timeout_s: int = 20,
    settle_sleep_s: float = 1.0,
    retries: int = 2,
    retry_sleep_s: float = 2.0,
    user_agent: Optional[str] = None,
    window_size: str = "1400,900",
) -> str:
    """Render a URL with Selenium/Chrome and return the final page HTML.

    Typical scraper usage:

        html = fetch_rendered_html(job_link, wait_css="main")
        soup = BeautifulSoup(html, "html.parser")

    `wait_css` should point at content that proves the JS-rendered detail page is ready.
    If omitted, the helper waits for document.readyState and the presence of <body>.
    """
    attempts = max(1, int(retries))
    last_error: Optional[BaseException] = None

    try:
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from urllib3.exceptions import ReadTimeoutError
    except ModuleNotFoundError as exc:
        raise RenderFetchError("Selenium render fetch requires selenium and urllib3 to be installed.") from exc

    for attempt in range(1, attempts + 1):
        driver = None
        try:
            driver = _make_driver(
                headless=headless,
                user_agent=user_agent,
                window_size=window_size,
            )
            driver.set_page_load_timeout(timeout_s)
            driver.get(url)

            wait = WebDriverWait(driver, timeout_s)
            wait.until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))

            if wait_css:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))
            else:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            if settle_sleep_s > 0:
                time.sleep(settle_sleep_s)

            html = driver.page_source or ""
            if not html.strip():
                raise RenderFetchError("Rendered page returned empty HTML.")

            return html

        except (
            TimeoutException,
            ReadTimeoutError,
            WebDriverException,
            RenderFetchError,
        ) as exc:
            last_error = exc
            logging.warning(
                "Rendered fetch failed for %s (attempt %s/%s): %s",
                url,
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                time.sleep(retry_sleep_s)
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    raise RenderFetchError(f"Failed to render {url} after {attempts} attempts: {last_error}") from last_error


def fetch_rendered_response(url: str, **kwargs) -> RenderedResponse:
    """Return rendered HTML wrapped in a tiny requests-like response object."""
    return RenderedResponse(url=url, text=fetch_rendered_html(url, **kwargs))


def fetch_rendered_soup(url: str, parser: str = "html.parser", **kwargs):
    """Render a URL and parse the HTML with BeautifulSoup."""
    from bs4 import BeautifulSoup

    return BeautifulSoup(fetch_rendered_html(url, **kwargs), parser)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a URL with Selenium and print HTML to stdout.")
    parser.add_argument("url", help="URL to render.")
    parser.add_argument("--wait-css", help="CSS selector to wait for before returning HTML.")
    parser.add_argument("--timeout", type=int, default=20, help="Page load and wait timeout in seconds.")
    parser.add_argument("--settle-sleep", type=float, default=1.0, help="Seconds to wait after readiness checks.")
    parser.add_argument("--retries", type=int, default=2, help="Render attempts before failing.")
    parser.add_argument("--retry-sleep", type=float, default=2.0, help="Seconds between render attempts.")
    parser.add_argument("--user-agent", help="Optional browser user agent.")
    parser.add_argument("--window-size", default="1400,900", help="Chrome window size, e.g. 1400,900.")
    parser.add_argument("--headed", action="store_true", help="Run Chrome with a visible browser window.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        html = fetch_rendered_html(
            args.url,
            wait_css=args.wait_css,
            headless=not args.headed,
            timeout_s=args.timeout,
            settle_sleep_s=args.settle_sleep,
            retries=args.retries,
            retry_sleep_s=args.retry_sleep,
            user_agent=args.user_agent,
            window_size=args.window_size,
        )
    except RenderFetchError as exc:
        logging.error("%s", exc)
        return 1

    sys.stdout.write(html)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
