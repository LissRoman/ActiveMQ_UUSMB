# src/services/watson/main.py
import logging
import signal
import sys
import time  # Necesario para el sleep durante el apagado

from ... import config  # Importar todo el módulo de configuración
from ...utils.logger import setup_logging  # Importar solo la función de setup
from ...utils.stomp_client import StompClient
from .listener import WatsonListener  # La importación relativa del listener está bien

from . import db


logger = setup_logging(
    config.WATSON_LOG_FILE, "WATSON"
)  

# Instancia global del cliente para que sea accesible desde el manejador de señales
stomp_client = None


def handle_shutdown_signal(signum, frame):
    """Apaga el cliente STOMP de forma ordenada al recibir SIGINT/SIGTERM."""
    logger.info(f"Se recibió la señal de apagado {signum}. Iniciando apagado ordenado...")
    if stomp_client:
        stomp_client.disconnect()  # Pedir al cliente que se desconecte
    # Dar tiempo para que se desconecte antes de salir
    time.sleep(2)
    logger.info("Apagado completo.")
    sys.exit(0)


def main():
    global stomp_client
    logger.info("--- Iniciando servicio Watson ---")
    logger.info(f"Registrando en: {config.WATSON_LOG_FILE}")  
    logger.info(f"Conectando a ActiveMQ en {config.HOST}:{config.PORT}")
    logger.info(f"Escuchando en la cola: {config.WATSON_QUEUE}")

    db.init_db()

    try:
        # Crear la instancia del listener
        listener = WatsonListener(
            None
        )  # Pasar None inicialmente, la referencia al cliente se establece en StompClient

        # Crear la instancia del cliente STOMP
        stomp_client = StompClient(
            host=config.HOST,
            port=config.PORT,
            username=config.USERNAME,
            password=config.PASSWORD,
            listener_instance=listener,  # Pasar la instancia del listener
        )

        # Registrar los manejadores de señales para apagado ordenado
        signal.signal(signal.SIGINT, handle_shutdown_signal)
        signal.signal(signal.SIGTERM, handle_shutdown_signal)

        # Conectarse (incluye lógica de reintento)
        stomp_client.connect()  # Lanza ConnectionError si falla de forma permanente

        # Suscribirse después de una conexión exitosa
        stomp_client.subscribe(destination=config.WATSON_QUEUE, id=1, ack="auto")

        logger.info("El servicio Watson está en ejecución. Esperando mensajes...")
        # Mantener la conexión activa y manejar reconexiones
        stomp_client.run_forever()  # Este bucle maneja KeyboardInterrupt internamente

    except ConnectionError as e:
        logger.critical(f"No se pudo establecer la conexión inicial: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Ocurrió un error crítico inesperado: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("--- Servicio Watson detenido ---")


if __name__ == "__main__":
    main()
