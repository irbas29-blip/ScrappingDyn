import asyncio
import re
from playwright.async_api import async_playwright
from datetime import datetime
from urllib.parse import urlparse, urljoin
import os
from bs4 import BeautifulSoup
import html2text
import requests
from utils import save_visited_url, sanitize_filename, clean_link_fragment
from config import TIMEOUTCALL, TIMEOUTWAIT, MAX_CONCURRENCY, UNWANTED_KEYWORDS
from logger import setup_error_logger, log_pdf_error, log_scraping_error, log_network_error

# Logger global pour ce module
error_logger = setup_error_logger("blog_scraper")

def download_pdf_sync(url, project_dir, visited_pages):
    """Télécharge un fichier PDF avec requests (même logique que fetch.py)"""
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

def detect_pagination_format(url):
    """
    Détecte le format de pagination d'une URL
    Retourne: ('query', 'page') pour ?page=X ou ('path', 'page') pour /page/X/
    """
    if "page=" in url:
        return "query", "page"
    elif re.search(r'/page/\d+/?', url):
        return "path", "page"
    else:
        # Fallback - essayer de détecter d'autres patterns
        return "query", "page"

def build_next_page_url(base_url, page_num, format_type, param_name):
    """
    Construit l'URL de la page suivante selon le format détecté
    """
    if format_type == "query":
        # Format: ?page=X ou &page=X
        if f"{param_name}=" in base_url:
            # Remplacer le numéro existant
            pattern = rf"({param_name}=)\d+"
            return re.sub(pattern, rf"\g<1>{page_num}", base_url)
        else:
            # Ajouter le paramètre
            separator = "&" if "?" in base_url else "?"
            return f"{base_url}{separator}{param_name}={page_num}"
    
    elif format_type == "path":
        # Format: /page/X/
        if f"/{param_name}/" in base_url:
            pattern = rf"(/{param_name}/)\d+(/?)$"
            return re.sub(pattern, rf"\g<1>{page_num}\g<2>", base_url)
        else:
            # Ajouter à la fin
            return f"{base_url.rstrip('/')}/{param_name}/{page_num}/"
    
    return base_url

async def extract_article_links(page, base_domain, article_selector):
    """
    Extrait les liens d'articles d'une page de listing
    """
    try:
        if article_selector:
            # Utiliser un sélecteur spécifique si fourni
            links = await page.evaluate(f'''() => {{
                const articles = document.querySelectorAll('{article_selector}');
                const links = new Set();
                articles.forEach(article => {{
                    const link = article.querySelector('a[href]');
                    if (link) {{
                        try {{
                            const url = new URL(link.href, document.baseURI).href;
                            links.add(url);
                        }} catch {{}}
                    }}
                }});
                return Array.from(links);
            }}''')
        else:
            # Extraction générique - tous les liens de la page
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
        
        # Filtrer les liens pour garder seulement ceux du même domaine
        filtered_links = []
        for link in links:
            try:
                clean_link = clean_link_fragment(link)
                parsed = urlparse(clean_link)
                
                # Vérifier si c'est du même domaine
                if parsed.netloc == base_domain:
                    # Exclure les URLs indésirables
                    if not any(keyword in clean_link.lower() for keyword in UNWANTED_KEYWORDS):
                        # Exclure les pages de pagination elles-mêmes
                        if not re.search(r'page[/=]\d+', clean_link.lower()):
                            filtered_links.append(clean_link)
            except Exception as e:
                log_scraping_error(error_logger, link, f"Link filtering error: {e}", "Link processing")
                continue
        
        return filtered_links
        
    except Exception as e:
        log_scraping_error(error_logger, "page", f"Link extraction error: {e}", "Link extraction")
        return []

async def scrape_single_article(url, main_div_name, keep_div_name, semaphore, project_dir, visited_pages, visited_urls_from_file):
    """
    Scrape un seul article (même logique que fetch_uniquepage mais simplifié)
    """
    async with semaphore:
        normalized_url = url.rstrip('/').lower()
        if normalized_url in visited_pages or normalized_url in visited_urls_from_file:
            return False
        
        # Vérifier si c'est un PDF
        if url.lower().endswith('.pdf'):
            download_pdf_sync(url, project_dir, visited_pages)
            return True
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                print(f"    📄 Scraping article: {url}")
                await page.goto(url, timeout=TIMEOUTCALL, wait_until="networkidle")
                await page.wait_for_timeout(TIMEOUTWAIT)

                final_url = (await page.evaluate("window.location.href")).lower().rstrip('/')
                if final_url != normalized_url and final_url in visited_pages:
                    await browser.close()
                    return False
                
                visited_pages.add(normalized_url)
                save_visited_url(normalized_url)

                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                # Gérer les attributs data-* vs classes pour main_div
                if main_div_name.startswith('data-'):
                    main_div = soup.find("div", attrs={main_div_name: True})
                else:
                    main_div = soup.find("div", class_=main_div_name)
                
                if main_div:
                    # Nettoyer les éléments indésirables de tout le document
                    for tag in soup(['script', 'style']):
                        tag.decompose()

                    # Si keep_div_name est spécifié, chercher ces blocs dans main_div
                    if keep_div_name:
                        content_blocks = main_div.find_all("div", class_=keep_div_name)
                        if content_blocks:
                            html_snippet = "\n".join(str(block) for block in content_blocks)
                        else:
                            # Fallback: essayer de trouver le contenu principal
                            # Pour Dynamics Community, chercher les divs avec le contenu de l'article
                            post_content = main_div.find("div", class_="post-content")
                            if post_content:
                                html_snippet = str(post_content)
                            else:
                                # Dernier recours: prendre tout le contenu de main_div mais nettoyer
                                for unwanted in main_div.find_all(['nav', 'footer', 'header', 'aside']):
                                    unwanted.decompose()
                                html_snippet = str(main_div)
                    else:
                        # Prendre tout le contenu de main_div mais nettoyer
                        for unwanted in main_div.find_all(['nav', 'footer', 'header', 'aside']):
                            unwanted.decompose()
                        html_snippet = str(main_div)
                    
                    # Convertir en markdown
                    converter = html2text.HTML2Text()
                    converter.ignore_links = False
                    converter.body_width = 0
                    article_md = converter.handle(html_snippet)
                    
                    # Extraire le titre de la page
                    title_tag = soup.find("title")
                    page_title = title_tag.get_text().strip() if title_tag else "Article"
                    
                    markdown_content = f"# {page_title}\n\n{article_md}"

                    # Sauvegarder
                    safe_filename = sanitize_filename(final_url)
                    os.makedirs(project_dir, exist_ok=True)
                    file_path = os.path.join(project_dir, f"{safe_filename}.md")

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(f"<!-- URL: {final_url} | Scraped at: {datetime.utcnow()} -->\n\n")
                        f.write(markdown_content)

                    print(f"    ✅ Article saved: {file_path}")
                    await browser.close()
                    return True
                else:
                    error_msg = f"Aucune section {main_div_name} trouvée"
                    print(f"    ❌ {error_msg}")
                    log_scraping_error(error_logger, url, error_msg, "Missing main container")
                    await browser.close()
                    return False

            except Exception as e:
                error_msg = f"Error scraping article {url}: {type(e).__name__}: {e}"
                print(f"    ⚠️ {error_msg}")
                
                if "timeout" in str(e).lower() or "net::" in str(e).lower():
                    log_network_error(error_logger, url, str(e))
                else:
                    log_scraping_error(error_logger, url, str(e), "Article scraping error")
                
                await browser.close()
                return False

async def fetch_blog_with_pagination(base_url, main_div_name, keep_div_name, article_selector, semaphore, project_dir, visited_pages, visited_urls_from_file):
    """
    Scrape un blog avec pagination
    
    Args:
        base_url: URL de la page 1 (ex: "https://example.com/page/1/" ou "https://example.com?page=1")
        main_div_name: Sélecteur pour le conteneur principal des articles
        keep_div_name: Sélecteur pour les blocs de contenu à garder (peut être vide)
        article_selector: Sélecteur CSS pour identifier les articles sur la page de listing (peut être vide)
        semaphore: Semaphore pour contrôler la concurrence
        project_dir: Dossier où sauvegarder
        visited_pages: Set des pages déjà visitées
        visited_urls_from_file: Set des URLs déjà visitées depuis le fichier
    """
    print(f"\n📚 Starting blog scrape with pagination: {base_url}")
    
    # Détecter le format de pagination
    format_type, param_name = detect_pagination_format(base_url)
    print(f"    📋 Pagination format detected: {format_type} with parameter '{param_name}'")
    
    # Extraire le domaine de base
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc
    
    current_page = 1
    total_articles_found = 0
    empty_pages_count = 0
    max_empty_pages = 3  # Arrêter après 3 pages vides consécutives
    
    while empty_pages_count < max_empty_pages:
        # Construire l'URL de la page courante
        if current_page == 1:
            current_url = base_url
        else:
            current_url = build_next_page_url(base_url, current_page, format_type, param_name)
        
        print(f"\n📖 Processing page {current_page}: {current_url}")
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()
                
                try:
                    await page.goto(current_url, timeout=TIMEOUTCALL, wait_until="networkidle")
                    await page.wait_for_timeout(TIMEOUTWAIT)
                    
                    # Extraire les liens d'articles de cette page
                    article_links = await extract_article_links(page, base_domain, article_selector)
                    
                    await browser.close()
                    
                    if not article_links:
                        empty_pages_count += 1
                        print(f"    ❌ No articles found on page {current_page} (empty count: {empty_pages_count})")
                        
                        # Si c'est la première page et qu'elle est vide, arrêter immédiatement
                        if current_page == 1:
                            print("    🛑 First page is empty, stopping.")
                            break
                    else:
                        empty_pages_count = 0  # Reset counter
                        print(f"    ✅ Found {len(article_links)} articles on page {current_page}")
                        
                        # Scraper chaque article
                        tasks = []
                        for article_url in article_links:
                            task = scrape_single_article(
                                article_url, 
                                main_div_name, 
                                keep_div_name, 
                                semaphore, 
                                project_dir, 
                                visited_pages, 
                                visited_urls_from_file
                            )
                            tasks.append(task)
                        
                        # Exécuter les tâches de scraping
                        if tasks:
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            successful_scrapes = sum(1 for r in results if r is True)
                            total_articles_found += successful_scrapes
                            print(f"    📊 Successfully scraped {successful_scrapes}/{len(article_links)} articles from page {current_page}")
                
                except Exception as e:
                    error_msg = f"Error accessing page {current_page}: {type(e).__name__}: {e}"
                    print(f"    ⚠️ {error_msg}")
                    log_network_error(error_logger, current_url, str(e))
                    await browser.close()
                    
                    # Si c'est une erreur de réseau, essayer la page suivante
                    empty_pages_count += 1
        
        except Exception as e:
            error_msg = f"Critical error on page {current_page}: {type(e).__name__}: {e}"
            print(f"    🚨 {error_msg}")
            log_scraping_error(error_logger, current_url, str(e), "Critical pagination error")
            empty_pages_count += 1
        
        current_page += 1
        
        # Protection contre les boucles infinies
        if current_page > 1000:
            print("    🛑 Maximum page limit reached (1000), stopping.")
            break
    
    print(f"\n🎉 Blog scraping completed!")
    print(f"    📊 Total pages processed: {current_page - 1}")
    print(f"    📄 Total articles scraped: {total_articles_found}")
    print(f"    📁 Articles saved in: {project_dir}")
