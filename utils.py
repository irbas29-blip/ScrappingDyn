import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
from urllib.parse import urlparse
import os
from config import VISITED_FILE, UNWANTED_KEYWORDS
from urllib.parse import urlparse, urlunparse

def clean_link_fragment(url):
    parsed = urlparse(url)
    cleaned = parsed._replace(fragment="")  # vide la partie #...
    return urlunparse(cleaned).rstrip('/')

def load_visited_urls():
    if not os.path.exists(VISITED_FILE):
        return set()
    with open(VISITED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip().split(" | ")[0] for line in f if line.strip())
    
def save_visited_url(url):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(VISITED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url} | {timestamp}\n")

def load_urls_from_csv(filepath):
    df = pd.read_csv(filepath)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    if 'scraped' not in df.columns:
        df['scraped'] = False
    df['scraped'] = df['scraped'].fillna(False).astype(bool)
    return df.to_dict('records')

def is_unwanted_url(url, base_url):
    if not url.startswith(base_url):
        return True
    url_lower = url.lower()
    return any(keyword in url_lower for keyword in UNWANTED_KEYWORDS)

def sanitize_filename(url):
    parsed = urlparse(url)
    safe_path = parsed.netloc + parsed.path
    safe_filename = safe_path.strip("/").replace("/", "_").replace("?", "_").replace("&", "_")
    return safe_filename