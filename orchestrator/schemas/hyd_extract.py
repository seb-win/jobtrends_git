import json
import os
import time
from typing import Any, Dict, Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib3.exceptions import ReadTimeoutError


class HydrationError(RuntimeError):
    """Raised when hydration data cannot be found or parsed."""
    pass


def _make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    chrome_binary = os.environ.get("CHROME_BINARY")
    if chrome_binary:
        opts.binary_location = chrome_binary

    if headless:
        # "new" headless is more stable for modern Chrome
        opts.add_argument("--headless=new")

    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")

    # Optional: reduce automation fingerprints a bit (not magic, but helps sometimes)
    opts.add_argument("--disable-blink-features=AutomationControlled")

    return webdriver.Chrome(options=opts)


def get_hydration_json(
    url: str,
    hydration_name: str,
    *,
    headless: bool = True,
    timeout_s: int = 20,
    wait_css: Optional[str] = None,
    script_tag_css: Optional[str] = None,
    retries: int = 3,
    retry_sleep_s: float = 2.0,
) -> Dict[str, Any]:
    """
    Load `url` in Selenium and extract hydration JSON.

    Parameters
    ----------
    url:
      Page URL to load.
    hydration_name:
      Name of the global variable on window, e.g. "__NEXT_DATA__", "__NUXT__", "__APOLLO_STATE__".
      IMPORTANT: pass the exact variable name without "window." prefix.
    headless:
      Run Chrome in headless mode.
    timeout_s:
      Selenium wait timeout.
    wait_css:
      Optional CSS selector to wait for. Use if hydration appears after render.
    script_tag_css:
      If hydration is in a script tag rather than on window, pass a CSS selector pointing to it,
      e.g. 'script#__NEXT_DATA__' or 'script[type="application/json"]#hydration'.

    Returns
    -------
    dict
      Parsed hydration JSON.
    """
    attempts = max(1, int(retries))
    last_error: Optional[BaseException] = None

    for attempt in range(1, attempts + 1):
        driver = None
        try:
            driver = _make_driver(headless=headless)
            driver.set_page_load_timeout(timeout_s)
            driver.get(url)

            # Optional wait for page readiness signal
            if wait_css:
                WebDriverWait(driver, timeout_s).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )

            # 1) Preferred: read from window[hydration_name]
            js_get_window_var = """
                const name = arguments[0];
                try {
                  const v = window[name];
                  if (v === undefined || v === null) return null;
                  return v;
                } catch (e) {
                  return null;
                }
            """
            val = driver.execute_script(js_get_window_var, hydration_name)

            # 2) Fallback: read from script tag
            if val is None:
                if not script_tag_css:
                    raise HydrationError(
                        f"Hydration '{hydration_name}' not found on window. "
                        f"Provide script_tag_css if the JSON is inside a script tag."
                    )

                el = WebDriverWait(driver, timeout_s).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, script_tag_css))
                )
                raw = (el.get_attribute("textContent") or el.get_attribute("innerHTML") or "").strip()
                if not raw:
                    raise HydrationError(f"Script tag '{script_tag_css}' is empty.")

                try:
                    return json.loads(raw)
                except json.JSONDecodeError as e:
                    raise HydrationError(f"Script tag JSON could not be parsed: {e}") from e

            # 3) Selenium already returned a dict -> done
            if isinstance(val, dict):
                return val

            # 4) If it's a JSON string -> parse
            if isinstance(val, str):
                s = val.strip()
                try:
                    return json.loads(s)
                except json.JSONDecodeError as e:
                    raise HydrationError(
                        f"Hydration '{hydration_name}' is a string but not valid JSON. "
                        f"First 200 chars: {s[:200]!r}"
                    ) from e

            # 5) If it's an array/list -> wrap (still JSON-valid)
            if isinstance(val, list):
                return {"_root": val}

            raise HydrationError(
                f"Hydration '{hydration_name}' found, but unsupported type: {type(val).__name__}"
            )

        except (TimeoutException, ReadTimeoutError, WebDriverException, HydrationError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(retry_sleep_s)
                continue
            break
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    raise HydrationError(
        f"Failed to extract hydration '{hydration_name}' from {url} after {attempts} attempts: {last_error}"
    ) from last_error


# Optional: quick manual test ONLY when running this file directly
if __name__ == "__main__":
    # Example:
    # data = get_hydration_json(
    #     "https://example.com",
    #     "__NEXT_DATA__",
    #     headless=True,
    #     script_tag_css="script#__NEXT_DATA__",
    # )
    # print(json.dumps(data, indent=2, ensure_ascii=False))
    pass
