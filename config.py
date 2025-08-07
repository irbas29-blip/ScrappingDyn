# Fichier CSV d'entrée
URLS_FILE_PATH = r"C:\Users\Admin\ARC CONSEIL\Communication site - Documents\03 Projets\04 Assistant IA Dynamics\Code Scrapping\WebScrapping V2\flat\url.csv"

OUTPUT_ROOT =  r"C:\Users\Admin\ARC CONSEIL\Communication site - Documents\03 Projets\04 Assistant IA Dynamics\Code Scrapping\WebScrapping V2\flat"

# Mots-clés pour ignorer certaines URLs
UNWANTED_KEYWORDS = [
    "print", "share", "login", "signin", "signup", "logout", 
    "facebook", "twitter", "linkedin", "cart", "checkout", 
    "contact", "Business Central"
]

VISITED_FILE = "visited.txt"

MAX_CONCURRENCY = 5

# Crée le dossier de sortie
OUTPUT_DIR = "scraped_articles"

TIMEOUTCALL = 60000
TIMEOUTWAIT = 2000