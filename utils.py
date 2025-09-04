import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
from urllib.parse import urlparse
import os
import hashlib
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

def sanitize_filename(url, max_length=100):
    """Crée un nom de fichier sûr à partir d'une URL"""
    try:
        parsed = urlparse(url)
        
        # Extraire la partie utile du chemin
        path_parts = [part for part in parsed.path.split('/') if part]
        
        # Prendre les 2-3 dernières parties du chemin
        if len(path_parts) >= 2:
            useful_parts = path_parts[-2:]  # 2 dernières parties
        elif len(path_parts) == 1:
            useful_parts = path_parts
        else:
            useful_parts = ["page"]
        
        # Créer le nom de base
        base_name = "_".join(useful_parts)
        
        # Nettoyer les caractères problématiques
        safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        cleaned_name = "".join(c if c in safe_chars else "_" for c in base_name)
        
        # Limiter la longueur
        if len(cleaned_name) > max_length:
            # Garder le début + hash de l'URL complète pour unicité
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            cleaned_name = cleaned_name[:max_length-9] + "_" + url_hash
        
        # S'assurer qu'il n'est pas vide
        if not cleaned_name or cleaned_name == "_":
            cleaned_name = f"page_{hashlib.md5(url.encode()).hexdigest()[:8]}"
        
        return cleaned_name
        
    except Exception as e:
        # En cas d'erreur, créer un nom basé sur le hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        return f"page_{url_hash}"
