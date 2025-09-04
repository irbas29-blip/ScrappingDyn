import asyncio
from playwright.async_api import async_playwright
from datetime import datetime
from urllib.parse import urlparse
import os
from bs4 import BeautifulSoup
import html2text
from utils import save_visited_url, sanitize_filename, clean_link_fragment
from config import TIMEOUTCALL, TIMEOUTWAIT, MAX_CONCURRENCY, UNWANTED_KEYWORDS
from logger import setup_error_logger, log_scraping_error, log_network_error

# Logger spécifique pour les blogs
blog_error_logger = setup_error_logger("blog_scraper")

async def fetch_blog_with_pagination(base_url, main_div_name, keep_div_name, articles_selector, semaphore, project_dir, visited_pages, visited_urls_from_file):
    """
    Parcourt un blog avec pagination et scrape chaque article
    
    Args:
        base_url: URL de base du blog (ex: https://blog.example.com/page/1)
        main_div_name: Sélecteur du conteneur principal des articles (ex: data-main-column)
        keep_div_name: Sélecteur du contenu à garder dans l'article (ex: content)
        articles_selector: Sélecteur pour trouver les liens d'articles (ex: article-links)
    """
    print(f"\n🗞️ Starting blog scrape: {base_url}")
    
    page_num = 1
    max_empty_pages = 3  # Arrêter après 3 pages vides consécutives
    empty_pages_count = 0
    total_articles_scraped = 0
    
    while empty_pages_count < max_empty_pages:
        # Construire l'URL de pagination
        pagination_url = build_pagination_url(base_url, page_num)
        
        print(f"\n📄 Page {page_num}: {pagination_url}")
        
        # Récupérer les liens d'articles de cette page
        article_links = await get_article_links_from_page(
            pagination_url, articles_selector, semaphore, visited_pages, visited_urls_from_file
        )
        
        if not article_links:
            empty_pages_count += 1
            print(f"    📭 Page {page_num} vide ou sans nouveaux articles ({empty_pages_count}/{max_empty_pages})")
        else:
            # Reset le compteur si on trouve des articles
            empty_pages_count = 0
            print(f"    🔗 Trouvé {len(article_links)} nouveaux articles sur la page {page_num}")
            
            # Scraper chaque article trouvé
            articles_scraped_this_page = 0
            for i, article_url in enumerate(article_links, 1):
                print(f"    📖 Article {i}/{len(article_links)}: {article_url}")
                
                success = await scrape_single_article(
                    article_url, main_div_name, keep_div_name, 
                    semaphore, project_dir, visited_pages, visited_urls_from_file
                )
                
                if success:
                    articles_scraped_this_page += 1
                    total_articles_scraped += 1
            
            print(f"    ✅ {articles_scraped_this_page}/{len(article_links)} articles scrapés avec succès")
        
        page_num += 1
        
        # Sécurité : arrêter après 200 pages
        if page_num > 200:
            print("⚠️ Arrêt sécuritaire après 200 pages")
            break
    
    print(f"\n🎉 Blog scraping terminé !")
    print(f"   📊 Pages parcourues: {page_num-1}")
    print(f"   📖 Articles scrapés: {total_articles_scraped}")

def build_pagination_url(base_url, page_num):
    """Construit l'URL avec pagination intelligente"""
    # Si c'est la page 1, retourner l'URL de base
    if page_num == 1:
        return base_url
    
    # Détecter le type de pagination
    if '/page/' in base_url:
        # Format: https://blog.com/page/1 → https://blog.com/page/2
        if base_url.endswith('/page/1'):
            base_without_page = base_url.replace('/page/1', '')
        else:
            base_without_page = base_url.split('/page/')[0]
        return f"{base_without_page}/page/{page_num}"
    elif 'page=' in base_url:
        # Format: https://blog.com?page=1 → https://blog.com?page=2
        if 'page=1' in base_url:
            return base_url.replace('page=1', f'page={page_num}')
        else:
            # Ajouter le paramètre page
            separator = '&' if '?' in base_url else '?'
            return f"{base_url}{separator}page={page_num}"
    else:
        # Format par défaut: ajouter /page/X
        return f"{base_url.rstrip('/')}/page/{page_num}"

async def get_article_links_from_page(pagination_url, articles_selector, semaphore, visited_pages, visited_urls_from_file):
    """Extrait uniquement les liens d'articles depuis une page de blog"""
    async with semaphore:
        normalized_url = pagination_url.rstrip('/').lower()
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                await page.goto(pagination_url, timeout=TIMEOUTCALL, wait_until="networkidle")
                await page.wait_for_timeout(TIMEOUTWAIT)
                
                # Extraire les liens d'articles avec JavaScript
                article_links = await extract_article_links(page, articles_selector)
                
                await browser.close()
                
                # Filtrer les liens pour éviter les doublons
                clean_links = []
                for link in article_links:
                    try:
                        clean_link = clean_link_fragment(link)
                        normalized_link = clean_link.rstrip('/').lower()
                        
                        # Éviter les doublons et les liens déjà visités
                        if (normalized_link not in visited_pages and 
                            normalized_link not in visited_urls_from_file and
                            not any(keyword in normalized_link for keyword in UNWANTED_KEYWORDS)):
                            clean_links.append(clean_link)
                    except Exception as e:
                        log_scraping_error(blog_error_logger, link, f"Link processing error: {e}", "Link filtering")
                        continue
                
                return clean_links
                
            except Exception as e:
                error_msg = f"Erreur récupération liens page {pagination_url}: {type(e).__name__}: {e}"
                print(f"    ⚠️ {error_msg}")
                
                if "timeout" in str(e).lower():
                    log_network_error(blog_error_logger, pagination_url, str(e))
                else:
                    log_scraping_error(blog_error_logger, pagination_url, str(e), "Blog pagination error")
                
                await browser.close()
                return []

async def extract_article_links(page, articles_selector):
    """Extrait les liens avec JavaScript en fonction du type de sélecteur"""
    if articles_selector.startswith('data-'):
        # Sélecteur d'attribut data-*
        return await page.evaluate(f'''() => {{
            const container = document.querySelector('[{articles_selector}]');
            if (!container) {{
                console.log('Container avec attribut {articles_selector} non trouvé');
                return [];
            }}
            
            const links = Array.from(container.querySelectorAll('a[href]'));
            console.log(`Trouvé ${{links.length}} liens dans le container {articles_selector}`);
            
            return links.map(a => {{
                try {{
                    return new URL(a.href, document.baseURI).href;
                }} catch {{
                    return null;
                }}
            }}).filter(url => url !== null);
        }}''')
    elif articles_selector.startswith('#'):
        # Sélecteur d'ID
        return await page.evaluate(f'''() => {{
            const container = document.querySelector('{articles_selector}');
            if (!container) {{
                console.log('Container avec ID {articles_selector} non trouvé');
                return [];
            }}
            
            const links = Array.from(container.querySelectorAll('a[href]'));
            console.log(`Trouvé ${{links.length}} liens dans le container {articles_selector}`);
            
            return links.map(a => {{
                try {{
                    return new URL(a.href, document.baseURI).href;
                }} catch {{
                    return null;
                }}
            }}).filter(url => url !== null);
        }}''')
    else:
        # Sélecteur de classe (par défaut)
        return await page.evaluate(f'''() => {{
            const container = document.querySelector('.{articles_selector}');
            if (!container) {{
                console.log('Container avec classe {articles_selector} non trouvé');
                return [];
            }}
            
            const links = Array.from(container.querySelectorAll('a[href]'));
            console.log(`Trouvé ${{links.length}} liens dans le container {articles_selector}`);
            
            return links.map(a => {{
                try {{
                    return new URL(a.href, document.baseURI).href;
                }} catch {{
                    return null;
                }}
            }}).filter(url => url !== null);
        }}''')

async def scrape_single_article(article_url, main_div_name, keep_div_name, semaphore, project_dir, visited_pages, visited_urls_from_file):
    """Scrape le contenu d'un article individuel"""
    async with semaphore:
        normalized_url = article_url.rstrip('/').lower()
        
        # Éviter les doublons
        if normalized_url in visited_pages or normalized_url in visited_urls_from_file:
            print(f"        ⚠️ Article déjà visité, ignoré")
            return False
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                await page.goto(article_url, timeout=TIMEOUTCALL, wait_until="networkidle")
                await page.wait_for_timeout(TIMEOUTWAIT)

                # Marquer comme visité
                visited_pages.add(normalized_url)
                save_visited_url(normalized_url)

                # Extraire le contenu
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                # Trouver le conteneur principal
                main_div = find_main_container(soup, main_div_name)
                
                if not main_div:
                    error_msg = f"Conteneur principal '{main_div_name}' non trouvé"
                    print(f"        ❌ {error_msg}")
                    log_scraping_error(blog_error_logger, article_url, error_msg, "Missing main container")
                    await browser.close()
                    return False
                
                # Nettoyer les éléments indésirables
                for tag in main_div(['script', 'style', 'nav', 'footer', 'header']):
                    tag.decompose()

                # Extraire le contenu spécifique
                content_blocks = main_div.find_all("div", class_=keep_div_name)
                if not content_blocks:
                    # Essayer sans restriction de classe si pas de blocs trouvés
                    content_blocks = [main_div]
                
                if content_blocks:
                    html_snippet = "\n".join(str(block) for block in content_blocks)
                    
                    # Convertir en Markdown
                    converter = html2text.HTML2Text()
                    converter.ignore_links = False
                    converter.body_width = 0
                    converter.unicode_snob = True
                    article_md = converter.handle(html_snippet)
                    
                    # Ajouter métadonnées
                    markdown_content = f"<!-- URL: {article_url} -->\n"
                    markdown_content += f"<!-- Scraped at: {datetime.utcnow()} -->\n\n"
                    markdown_content += article_md

                    # Sauvegarder
                    safe_filename = sanitize_filename(article_url)
                    os.makedirs(project_dir, exist_ok=True)
                    file_path = os.path.join(project_dir, f"{safe_filename}.md")

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(markdown_content)

                    print(f"        ✅ Article sauvé: {os.path.basename(file_path)}")
                    await browser.close()
                    return True
                else:
                    error_msg = f"Aucun contenu trouvé avec le sélecteur '{keep_div_name}'"
                    print(f"        ❌ {error_msg}")
                    log_scraping_error(blog_error_logger, article_url, error_msg, "Missing content blocks")
                    await browser.close()
                    return False

            except Exception as e:
                error_msg = f"Erreur scraping article {article_url}: {type(e).__name__}: {e}"
                print(f"        ⚠️ {error_msg}")
                
                if "timeout" in str(e).lower():
                    log_network_error(blog_error_logger, article_url, str(e))
                else:
                    log_scraping_error(blog_error_logger, article_url, str(e), "Article scraping error")
                
                await browser.close()
                return False

def find_main_container(soup, main_div_name):
    """Trouve le conteneur principal selon le type de sélecteur"""
    if main_div_name.startswith('data-'):
        # Attribut data-*
        return soup.find("div", attrs={main_div_name: True})
    elif main_div_name.startswith('#'):
        # ID
        return soup.find("div", id=main_div_name[1:])
    else:
        # Classe (par défaut)
        return soup.find("div", class_=main_div_name)