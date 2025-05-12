# src/services/liss/main.py
import logging
import signal
import sys
import time  # Necesario para dormir en el apagado
from ... import config, constants
from ...utils.logger import setup_logging
from ...utils.stomp_client import StompClient
from .listener import LISSListener

# Configuración del logging
logger = setup_logging(config.LISS_LOG_FILE, "LISS")

# Instancia global del cliente
stomp_client = None


def handle_shutdown_signal(signum, frame):
    """Apaga el cliente STOMP de forma ordenada."""
    logger.info(f"Se recibió la señal de apagado {signum}. Iniciando el apagado de LISS...")
    if stomp_client:
        stomp_client.disconnect()
    # Permitir tiempo para la desconexión
    time.sleep(2)
    logger.info("Apagado de LISS completado.")
    sys.exit(0)


def main():
    global stomp_client
    logger.info("--- Iniciando el Servicio de Orquestación LISS ---")
    logger.info(f"Registrando en: {config.LISS_LOG_FILE}")
    logger.info(f"Conectando a ActiveMQ en {config.HOST}:{config.PORT}")
    logger.info(f"Escuchando en la cola: {config.LISS_QUEUE}")

    try:
        listener = LISSListener(None)
        stomp_client = StompClient(
            host=config.HOST,
            port=config.PORT,
            username=config.USERNAME,
            password=config.PASSWORD,
            listener_instance=listener,
        )

        # Registrar manejadores de señales
        signal.signal(signal.SIGINT, handle_shutdown_signal)
        signal.signal(signal.SIGTERM, handle_shutdown_signal)

        # Conectar (incluye lógica de reintentos)
        stomp_client.connect()

        # Suscribirse
        stomp_client.subscribe(destination=config.LISS_QUEUE, id=1, ack="auto")

        logger.info("El servicio LISS está en ejecución. Esperando mensajes...")
        stomp_client.run_forever()

    except ConnectionError as e:
        logger.critical(f"No se pudo establecer la conexión inicial con ActiveMQ: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(
            f"Ocurrió un error crítico inesperado en el main de LISS: {e}", exc_info=True
        )
        sys.exit(1)
    finally:
        logger.info("--- Servicio LISS detenido ---")


if __name__ == "__main__":
    main()
