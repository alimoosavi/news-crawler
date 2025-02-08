import functools
import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import re


def retry_on_exception(retries=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except:
                    attempt += 1
                    time.sleep(delay)
            return None

        return wrapper

    return decorator


def get_web_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Enables headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")  # For better performance in headless mode

    return webdriver.Chrome(options=chrome_options)


@retry_on_exception(retries=3, delay=1)  # retry on failure with 1 second delay
def fetch(link):
    # Ensure the link has a protocol (http or https)
    if not re.match(r"^(http|https)://", link):
        link = "https://" + link  # Default to HTTPS

    try:
        response = requests.get(link, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200:
            return response.text  # Return HTML content
        else:
            print(f"Error: Received status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None


def run_curl_command(url):
    command = f"curl -s {url}"

    with os.popen(command) as stream:
        output = stream.read()

    return output
