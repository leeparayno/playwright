import time
import re
from pyotp import TOTP
from playwright.sync_api import sync_playwright, expect, BrowserContext
import logging

def test_sfccauth(browser_name: str = "chromium"):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)    
    with sync_playwright() as p:
        if browser_name.lower() == "chromium":
            browser = p.chromium.launch()
        elif browser_name.lower() == "firefox":
            browser = p.firefox.launch()
        elif browser_name.lower() == "webkit":
            browser = p.webkit.launch()
        else:
            raise ValueError(f"Unsupported browser: {browser_name}")

        # Create a new context with specific options
        context = browser.new_context(
            storage_state="auth.json",  # Load stored cookies and localStorage
            viewport={'width': 1280, 'height': 720},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        
        # Create a new page in this context
        page = context.new_page()

        username = "f-shopdisney-sfcc@disney.com"
        password = "DRTTECH0PS2024@!"
        totp_token = "25JSUNOROBA2GFCBUD72LJKZV3LOEHC4"

        page.goto("https://ccac.analytics.commercecloud.salesforce.com/login", wait_until="networkidle")
        time.sleep(4)
        page.fill('input[placeholder="User Name"]', username)
        page.click('#loginButton_0')
        time.sleep(4)
        page.fill('input[placeholder="Password"]', password)
        page.click('#loginButton_0')
        time.sleep(8)

        # screenshot
        page.screenshot(path="login.png")

        totp = TOTP(totp_token)
        token = totp.now()
        print(f"info: TOTP token generation complete: {token}")

        page.fill('#input-9', token)
        page.press('#input-9', 'Enter')
        time.sleep(8)

        cookies = page.context.cookies()
        sfcc_token = next((cookie['value'] for cookie in cookies if cookie['name'] == 'connect.sid'), "")
        xsrf_token = next((cookie['value'] for cookie in cookies if cookie['name'] == 'XSRF-TOKEN'), "")

        # screenshot
        page.screenshot(path="mainscreen.png")

        logger.debug(f"info: cookie received connect.sid:{sfcc_token}")
        logger.debug(f"debug: cookie received xsrf:{xsrf_token}")

        # At the end of your test, you can save the storage state
        context.storage_state(path="auth.json")

        # Close the browser
        browser.close()

if __name__ == "__main__":
    # You can change this to "firefox" or "webkit" to use a different browser
    test_sfccauth("chromium")