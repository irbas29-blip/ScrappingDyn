import asyncio
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime
from urllib.parse import urlparse
import os
import urllib.parse
from bs4 import BeautifulSoup
import html2text
from utils import save_visited_url, sanitize_filename, clean_link_fragment
from config import TIMEOUTCALL, TIMEOUTWAIT, MAX_CONCURRENCY, UNWANTED_KEYWORDS

async def fetch_uniquepage(url, base_url, main_div_name, keep_div_name, semaphore, project_dir, visited_pages, visited_urls_from_file):
    async with semaphore:
        normalized_url = url.rstrip('/').lower()
        if normalized_url in visited_pages:
            return []
        
        if normalized_url in visited_pages or normalized_url in visited_urls_from_file:
            print(f"    ❌ Already scraped (from file): {normalized_url}")
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
                # 🔹 Extraire uniquement les <div class="content"> dans <div data-main-column>
                main_div = soup.find("div", class_=main_div_name)
                if main_div:
                    for tag in main_div(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()

                    content_blocks = main_div.find_all("div", class_=keep_div_name)
                    if content_blocks:
                        html_snippet = "\n".join(str(block) for block in content_blocks)
                        # 🔹 Convertir en Markdown
                        converter = html2text.HTML2Text()
                        converter.ignore_links = False
                        converter.body_width = 0
                        article_md = converter.handle(html_snippet)
                        markdown_content = f"# {article_md}"

                        # 🔹 Enregistrer le contenu dans un fichier Markdown
                        safe_filename = sanitize_filename(final_url)
                        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")                
                        os.makedirs(project_dir, exist_ok=True)
                        file_path = os.path.join(project_dir, f"{safe_filename}.md")

                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(f"<!-- URL: {final_url} | Scraped at: {datetime.utcnow()} -->\n\n")
                            f.write(markdown_content)

                        print(f"    ✅ Saved: {file_path}")
                    else:
                        print(f"    ❌ Pas de blocs .content trouvés dans data-main-column")                        
                else:
                    print(f"    ❌ Aucune section data-main-column trouvée")            

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
                        print(f"    Check: {link}")
                        try:
                            clean_link = clean_link_fragment(link)
                            if clean_link.startswith(base_url) \
                            and clean_link not in visited_pages \
                            and not any(k in clean_link for k in UNWANTED_KEYWORDS):
                                filtered_links.append(clean_link)
                        except:
                            continue
                    return filtered_links

                await browser.close()

            except Exception as e:
                print(f"    ⚠️ Error fetching {normalized_url}: {type(e).__name__}: {e}")
                await browser.close()
                return []

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