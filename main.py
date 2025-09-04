import asyncio
import os
from utils import load_visited_urls, load_urls_from_csv
from config import MAX_CONCURRENCY, URLS_FILE_PATH, OUTPUT_ROOT
from fetch import fetch_pages_base
from fetch_blog import fetch_blog_with_pagination  # ✅ Nouveau import


async def main():
    urls_data = load_urls_from_csv(URLS_FILE_PATH)
    visited_pages = set()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    visited_urls_from_file = load_visited_urls()

    for entry in urls_data:
        source = entry.get('source', 'default')
        type = entry.get('type', 'default')
        url = entry['url'].rstrip('/')
        param1 = entry.get('param1', '')
        param2 = entry.get('param2', '')
        param3 = entry.get('param3', '')

        # ✅ Sécuriser le nom du dossier
        safe_source = source.replace("/", "_").replace("\\", "_").replace(" ", "_")
        project_dir = os.path.join(OUTPUT_ROOT, safe_source)

        match type:
            case "Base":
                await fetch_pages_base(url, param1, param2, semaphore, project_dir, visited_pages, visited_urls_from_file)
            case "Blog":  # ✅ Nouveau case
                await fetch_blog_with_pagination(url, param1, param2, param3, semaphore, project_dir, visited_pages, visited_urls_from_file)
            case "stop":
                print("Stopping...")
            case "pause":
                print("Pausing...")
            case _:
                print("Unknown command") 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            asyncio.create_task(main())
        else:
            raise