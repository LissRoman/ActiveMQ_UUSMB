import logging
import signal
import sys
import time  # Necesario para dormir durante el apagado

from ... import config  # Importa todo el módulo de configuración
from ...utils.logger import setup_logging  # Importa solo la función de configuración de logging
from ...utils.stomp_client import StompClient
from .listener import ClusterListener  # La importación relativa del listener es correcta

from . import db  # Importa el módulo de base de datos
logger = setup_logging(config.CLUSTER_LOG_FILE, "CLUSTER")

# Instancia global del cliente para que sea accesible por el manejador de señales
stomp_client = None


def handle_shutdown_signal(signum, frame):
    """Apaga el cliente STOMP de forma ordenada al recibir SIGINT/SIGTERM."""
    logger.info(f"Se recibió la señal de apagado {signum}. Iniciando apagado ordenado...")
    if stomp_client:
        stomp_client.disconnect()  # Solicita al cliente que se detenga y se desconecte
    # Da algo de tiempo para la desconexión antes de salir
    time.sleep(2)
    logger.info("Apagado del Cluster completado.")
    sys.exit(0)


def main():
    global stomp_client
    logger.info("--- Iniciando el servicio de Cluster ---")
    logger.info(f"Registrando logs en: {config.CLUSTER_LOG_FILE}")
    logger.info(f"Conectando a ActiveMQ en {config.HOST}:{config.PORT}")
    logger.info(f"Escuchando en la cola: {config.CLUSTER_QUEUE}")

    db.init_db()  # Inicializa la base de datos

    try:
        # Crea primero la instancia del listener
        listener = ClusterListener(
            None
        )  # Pasa None inicialmente, la referencia del cliente se establece en StompClient

        # Crea la instancia de StompClient
        stomp_client = StompClient(
            host=config.HOST,
            port=config.PORT,
            username=config.USERNAME,
            password=config.PASSWORD,
            listener_instance=listener,  # Pasa la instancia del listener
        )

        # Registra los manejadores de señales para un apagado ordenado
        signal.signal(signal.SIGINT, handle_shutdown_signal)
        signal.signal(signal.SIGTERM, handle_shutdown_signal)

        # lógica de reintento
        stomp_client.connect()  # Lanza ConnectionError si falla permanentemente

        # Suscríbete después de una conexión exitosa
        # Asegúrate de que config.CLUSTER_QUEUE esté definido
        stomp_client.subscribe(destination=config.CLUSTER_QUEUE, id=1, ack="auto")

        logger.info("El servicio de Cluster está en ejecución. Esperando mensajes...")
        # Mantén viva la conexión y maneja las reconexiones
        stomp_client.run_forever()  # Este bucle ahora maneja KeyboardInterrupt internamente

    except ConnectionError as e:
        logger.critical(f"No se pudo establecer la conexión inicial: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Ocurrió un error crítico inesperado: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("--- Servicio de Cluster detenido ---")


if __name__ == "__main__":
    main()
