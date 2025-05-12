# src/utils/formatter.py
from colorama import Fore, Style
import logging



def format_console_message(level, service_name, text):
    """Formats a console message for direct console output with color based on the logging level."""
    color = Style.RESET_ALL
    prefix_str = ""  # Variable para guardar el texto del prefijo como string

    # Primero, determina la cadena de texto del prefijo basada en el nivel
    if isinstance(level, int):
        # Si es un entero, intenta obtener el nombre del nivel de logging
        prefix_str = logging.getLevelName(level)
        # logging.getLevelName puede devolver el propio entero si el nivel es desconocido
        if isinstance(prefix_str, int):
            prefix_str = (
                f"LEVEL_{level}"  # Prefijo por defecto si no se conoce el nivel
            )
        prefix_str = str(prefix_str).upper()  # Asegura que es string y mayúsculas

    elif isinstance(level, str):
        # Si es una cadena, se usa como prefijo (con mayúsculas)
        prefix_str = level.upper()
        # Maneja el caso especial "SERVICE" para usar el service_name
        if prefix_str == "SERVICE":
            prefix_str = service_name.upper()

    else:
        # Para cualquier otro tipo inesperado, solo convierte a string
        prefix_str = str(level)

    # Ahora, determina el color basándose en el nivel original
    if level == logging.INFO:
        color = Fore.GREEN
    elif level == logging.WARNING:
        color = Fore.YELLOW
    elif level == logging.ERROR:
        color = Fore.RED
    elif level == logging.DEBUG:
        color = Fore.CYAN
    elif (
        isinstance(level, str) and level.upper() == "SERVICE"
    ):  # Compara con la cadena mayúscula
        color = Fore.MAGENTA
    # Añade más elifs aquí para otros niveles (enteros o cadenas) que necesiten color específico

    # Imprime la cadena formateada usando el prefijo_str y el color determinado
    print(f"{color}{prefix_str}:{Style.RESET_ALL} {text}")


def format_dict_for_console(data_dict, keys_to_show=None):
    """Formats a dictionary nicely for console output."""
    if not isinstance(data_dict, dict):
        return str(data_dict)

    if keys_to_show:
        items = [
            f"{Fore.BLUE}{k}{Style.RESET_ALL}: {v}"
            for k, v in data_dict.items()
            if k in keys_to_show
        ]
    else:  
        items = [f"{Fore.BLUE}{k}{Style.RESET_ALL}: {v}" for k, v in data_dict.items()]

    return ", ".join(items)

