from seleniumwire2 import webdriver 
from seleniumwire2 import SeleniumWireOptions
from seleniumwire2 import ProxyConfig  
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from src.config.settings import LOGGER, PROXY

import requests
import json 
import time
import traceback

API_URL = 'https://api.sgx.com/announcements/v1.1/?periodstart=20051005_160000&periodend=20251006_155959&cat=ANNC&sub=ANNC13&pagestart=0&pagesize=20'


def get_wire_driver(is_headless: bool = True, proxy: str | None = None) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()

    if is_headless:
        print("Running in headless mode...")
        options.add_argument("--headless=new")  
    else:
        print("Running in non-headless mode...")

    # Common options for stealth
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Window size
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    
    # Additional options
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    
    if proxy:
        print(f"Configuring browser with proxy: {proxy.split('@')[-1] if '@' in proxy else proxy}")
        seleniumwire_options = SeleniumWireOptions(
            request_storage='memory',
            verify_ssl=False,
            disable_encoding=True,
            upstream_proxy=ProxyConfig(
                http=proxy,
                https=proxy,
            )
        )
    else:
        seleniumwire_options = SeleniumWireOptions(
            request_storage='memory',
            verify_ssl=False,
            disable_encoding=True,
        )

    try:
        service = Service(ChromeDriverManager().install())
        
        driver = webdriver.Chrome(
            service=service, 
            options=options,
            seleniumwire_options=seleniumwire_options
        )
        
        # Hide automation markers
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        })
        
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        driver.execute_script("""
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)
        
        return driver
        
    except Exception as error:
        print(f"Failed to create driver: {error}")
        print(traceback.format_exc())
        return None


def get_auth(proxy: str | None = None) -> dict[str, str] | None:
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'authorizationtoken': '',
        'origin': 'https://www.sgx.com',
        'referer': 'https://www.sgx.com/',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    driver = None
    try:
        driver = get_wire_driver(is_headless=True, proxy=proxy)
        
        if not driver:
            return None
        
        print("Navigating to SGX page...")
        driver.get('https://www.sgx.com/securities/company-announcements?ANNC=ANNC13')
        
        # Check if blocked
        page_title = driver.title.lower()
        if 'access denied' in page_title or 'blocked' in page_title:
            print(f"Access denied! Page title: {driver.title}")
            return None
        
        print(f"Page loaded successfully. Title: {driver.title}")
        
        print("Waiting for JavaScript to execute...")
        time.sleep(8)
        
        # Interact with page
        try:
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Scroll
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(2)
            
        except Exception as error:
            print(f"Interaction warning: {error}")
        
        print("Waiting for API request...")
        
        try:
            request = driver.wait_for_request(
                'api.sgx.com/announcements',
                timeout=30
            )
            
            token = request.headers.get('authorizationtoken')
            if not token:
                print("No Authorization header found.")
                return None
            
            print(f"Token captured: {token[:20]}...")
            headers["authorizationtoken"] = token
            return headers
            
        except Exception as error:
            print(f"Timeout: {error}")
            
            # Debug
            print("\nDebug - Requests to SGX:")
            sgx_requests = [req for req in driver.requests if 'sgx' in req.url.lower()]
            
            if not sgx_requests:
                print("  No requests captured!")
            else:
                for req in sgx_requests[-10:]:
                    status = req.response.status_code if req.response else 'No response'
                    print(f"  {req.method} {req.url[:80]}... - {status}")
            
            return None

    except Exception as error:
        print(f"FAILED getting auth token: {error}")
        return None

    finally:
        if driver:
            print("Closing driver...")
            driver.quit()


def get_json(headers: dict[str, str] | None, proxy: str | None = None):
    if not headers:
        print("Cannot fetch JSON, headers missing.")
        return None
    
    proxies = None
    if proxy:
        proxies = {
            'http': proxy,
            'https': proxy,
        }
    
    try:
        print("Fetching data from API...")
        response = requests.get(API_URL, headers=headers, proxies=proxies, verify=False, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"Fetched {len(data.get('data', []))} announcements")
        return data

    except requests.exceptions.RequestException as error:
        print(f"API request failed: {error}")
        if 'response' in locals():
            print(f"Response: {response.text[:200]}")
        return None


if __name__ == '__main__':
    headers = get_auth(proxy=None)
    print(headers)
    data = get_json(headers=headers, proxy=None)
    print(data)
    announcements = data.get('data', [])
    print(json.dumps(announcements, indent=2))