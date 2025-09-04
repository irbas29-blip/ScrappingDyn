import os
import logging
from datetime import datetime
from config import OUTPUT_ROOT

def setup_error_logger(project_name="scraper"):
    """Configure le logger pour les erreurs de scraping"""
    
    # Créer le dossier de logs
    log_dir = os.path.join(OUTPUT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Nom du fichier avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{project_name}_errors_{timestamp}.log")
    
    # Configuration du logger
    logger = logging.getLogger('scraper_errors')
    logger.setLevel(logging.ERROR)
    
    # Éviter les doublons si le logger existe déjà
    if logger.handlers:
        logger.handlers.clear()
    
    # Handler pour fichier
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.ERROR)
    
    # Format des logs
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    return logger

def log_error(logger, url, error_type, error_message, context=""):
    """Log une erreur avec URL et détails"""
    error_entry = f"URL: {url} | ERROR_TYPE: {error_type} | MESSAGE: {error_message}"
    if context:
        error_entry += f" | CONTEXT: {context}"
    
    logger.error(error_entry)

def log_pdf_error(logger, url, error_message):
    """Log spécifique pour les erreurs PDF"""
    log_error(logger, url, "PDF_DOWNLOAD", error_message, "PDF download failed")

def log_scraping_error(logger, url, error_message, context="HTML scraping"):
    """Log spécifique pour les erreurs de scraping HTML"""
    log_error(logger, url, "SCRAPING", error_message, context)

def log_network_error(logger, url, error_message):
    """Log spécifique pour les erreurs réseau"""
    log_error(logger, url, "NETWORK", error_message, "Network/timeout error")