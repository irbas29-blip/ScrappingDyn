import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
from urllib.parse import urlparse
import os
import urllib.parse
from bs4 import BeautifulSoup
import html2text
import requests
import aiohttp
from utils import save_visited_url, sanitize_filename, clean_link_fragment
from config import TIMEOUTCALL, TIMEOUTWAIT, MAX_CONCURRENCY, UNWANTED_KEYWORDS
from logger import setup_error_logger, log_pdf_error, log_scraping_error, log_network_error

# Logger global pour ce module
error_logger = setup_error_logger("scraper")

def download_pdf_sync(url, project_dir, visited_pages):
    """Télécharge un fichier PDF avec requests"""
    try:
        normalized_url = url.rstrip('/').lower()
        if normalized_url in visited_pages:
            return
        
        print(f"    📄 Téléchargement PDF: {url}")
        
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Créer le nom de fichier
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename.endswith('.pdf'):
            filename = sanitize_filename(url) + '.pdf'
        
        # Créer le dossier PDF
        pdf_dir = os.path.join(project_dir, "PDFs")
        os.makedirs(pdf_dir, exist_ok=True)
        
        # Sauvegarder le PDF
        file_path = os.path.join(pdf_dir, filename)
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"    ✅ PDF sauvé: {file_path}")
        visited_pages.add(normalized_url)
        save_visited_url(normalized_url)
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Erreur réseau PDF {url}: {type(e).__name__}: {e}"
        print(f"    ⚠️ {error_msg}")
        log_pdf_error(error_logger, url, str(e))
    except OSError as e:
        error_msg = f"Erreur fichier PDF {url}: {type(e).__name__}: {e}"
        print(f"    ⚠️ {error_msg}")
        log_pdf_error(error_logger, url, f"File system error: {e}")
    except Exception as e:
        error_msg = f"Erreur PDF {url}: {type(e).__name__}: {e}"
        print(f"    ⚠️ {error_msg}")
        log_pdf_error(error_logger, url, str(e))

# On filtre sur les pages où il y a la balise html main et on récupère la balise de l'article uniquement div_name
# Le résultat est transformé en markdown et enregistré dans un fod
async def fetch_uniquepage(url, base_url, main_div_name, keep_div_name, semaphore, project_dir, visited_pages, visited_urls_from_file):
    async with semaphore:
        normalized_url = url.rstrip('/').lower()
        if normalized_url in visited_pages or normalized_url in visited_urls_from_file:
            return []
        
        # Vérifier si c'est un PDF
        if url.lower().endswith('.pdf'):
            download_pdf_sync(url, project_dir, visited_pages)
            return []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                print(f"    🌐 Visiting: {normalized_url}")
                await page.goto(url, timeout=TIMEOUTCALL, wait_until="networkidle")
                await page.wait_for_timeout(TIMEOUTWAIT)

                final_url = (await page.evaluate("window.location.href")).lower().rstrip('/')
                if final_url != normalized_url and final_url in visited_pages:
                    await browser.close()
                    return []
                
                visited_pages.add(normalized_url)
                save_visited_url(normalized_url)

                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                # Gérer les attributs data-* vs classes
                if main_div_name.startswith('data-'):
                    main_div = soup.find("div", attrs={main_div_name: True})
                else:
                    main_div = soup.find("div", class_=main_div_name)
                
                if main_div:
                    for tag in main_div(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()

                    content_blocks = main_div.find_all("div", class_=keep_div_name)
                    if content_blocks:
                        html_snippet = "\n".join(str(block) for block in content_blocks)
                        converter = html2text.HTML2Text()
                        converter.ignore_links = False
                        converter.body_width = 0
                        article_md = converter.handle(html_snippet)
                        markdown_content = f"# {article_md}"

                        safe_filename = sanitize_filename(final_url)
                        os.makedirs(project_dir, exist_ok=True)
                        file_path = os.path.join(project_dir, f"{safe_filename}.md")

                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(f"<!-- URL: {final_url} | Scraped at: {datetime.utcnow()} -->\n\n")
                            f.write(markdown_content)

                        print(f"    ✅ Saved: {file_path}")
                    else:
                        error_msg = f"Pas de blocs .{keep_div_name} trouvés"
                        print(f"    ❌ {error_msg}")
                        log_scraping_error(error_logger, url, error_msg, "Missing content blocks")
                else:
                    error_msg = f"Aucune section {main_div_name} trouvée"
                    print(f"    ❌ {error_msg}")
                    log_scraping_error(error_logger, url, error_msg, "Missing main container")

                # Récupérer les liens
                if base_url:
                    links = await page.evaluate('''() => {
                        const set = new Set();
                        document.querySelectorAll('a[href]').forEach(a => {
                            try {
                                const link = new URL(a.href, document.baseURI).href;
                                set.add(link);
                            } catch {}
                        });
                        return Array.from(set);
                    }''')

                    filtered_links = []
                    for link in links:
                        try:
                            clean_link = clean_link_fragment(link)
                            if clean_link.startswith(base_url) \
                            and clean_link not in visited_pages \
                            and not any(k in clean_link for k in UNWANTED_KEYWORDS):
                                filtered_links.append(clean_link)
                        except Exception as e:
                            log_scraping_error(error_logger, link, f"Link processing error: {e}", "Link filtering")
                            continue
                    
                    await browser.close()
                    return filtered_links

                await browser.close()

            except Exception as e:
                error_msg = f"Error fetching {normalized_url}: {type(e).__name__}: {e}"
                print(f"    ⚠️ {error_msg}")
                
                # Déterminer le type d'erreur
                if "timeout" in str(e).lower() or "net::" in str(e).lower():
                    log_network_error(error_logger, url, str(e))
                else:
                    log_scraping_error(error_logger, url, str(e), "General scraping error")
                
                await browser.close()
                return []

# fetch_pages_base : on par d'une URL de base et on scrappe tout ce qu'il y a en dessous
# Pour chaque lien, on regarde les autres liens mentionnés, si même url de base alors à scraper
async def fetch_pages_base(base_url, main_div_name, keep_div_name, semaphore, project_dir, visited_pages, visited_urls_from_file):
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    print(f"\n📌 Starting scrape of: {base_url}")
    queue = [base_url]

    while queue:
        current_url = queue.pop(0)
        filtered_links = await fetch_uniquepage(current_url, base_url, main_div_name, keep_div_name, semaphore, project_dir, visited_pages, visited_urls_from_file)
        if filtered_links:            
            for link in filtered_links:
                if link not in queue :
                    queue.append(link)