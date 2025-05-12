# src/utils/logger.py
import logging
from colorama import init, Fore, Style

# Inicializa colorama una sola vez
init(autoreset=True)

# Formato estándar de los logs
LOG_FORMAT = "%(levelname)s: %(asctime)s: %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_filename, logger_name, level=logging.DEBUG):
    # Configura el logger para el módulo especificado
    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():  # Evita añadir múltiples handlers si ya existen
        logger.handlers.clear()

    logger.setLevel(level)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Handler para archivo
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Handler para consola 
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        formatter
    ) 
    logger.addHandler(console_handler)

    return logger

