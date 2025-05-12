# src/services/sisbi/main.py
import logging
import signal
import sys
import threading
import time
import uvicorn
from contextlib import asynccontextmanager
from ... import config, constants
from ...utils.logger import setup_logging
from ...utils.stomp_client import StompClient
from .listener import SISBIListener
from .api import app  # Importar la instancia de la aplicación FastAPI


# Configuración del logging
logger = setup_logging(config.SISBI_LOG_FILE, "SISBI")

# Instancias globales
stomp_client = None
stomp_thread = None
shutdown_event = threading.Event()  # Usar un evento para señalizar el apagado


def run_stomp_client():
    """Función objetivo para el hilo del cliente STOMP."""
    global stomp_client
    try:
        logger.info("Hilo del cliente STOMP iniciado.")
        listener = SISBIListener(None)
        stomp_client = StompClient(
            host=config.HOST,
            port=config.PORT,
            username=config.USERNAME,
            password=config.PASSWORD,
            listener_instance=listener,
        )
        stomp_client.connect()
        stomp_client.subscribe(destination=config.SISBI_QUEUE, id=1, ack="auto")
        logger.info("Cliente STOMP conectado y suscrito. Ejecutando ciclo...")

        # comprobación para el shutdown_event dentro del ciclo de simulación
        while not shutdown_event.is_set():
            if not stomp_client.is_connected():
                logger.warning(
                    "Conexión STOMP perdida en el ciclo principal. Intentando reconectar."
                )
                try:
                    stomp_client.connect()  # La lógica de reconexión está dentro de connect()
                except ConnectionError:
                    logger.error(
                        "Reconexión STOMP fallida permanentemente en el hilo. Deteniendo el hilo."
                    )
                    break  # Salir del ciclo si la reconexión falla

            shutdown_event.wait(timeout=1.0)  # Esperar 1 segundo o hasta que el evento se active

        logger.info("Señal de apagado recibida en el hilo STOMP. Desconectando...")
        if stomp_client:
            stomp_client.disconnect()
        logger.info("Hilo del cliente STOMP finalizado.")

    except ConnectionError as e:
        logger.critical(
            f"Hilo del cliente STOMP: No se pudo establecer la conexión inicial: {e}"
        )

    except Exception as e:
        logger.critical(
            f"Hilo del cliente STOMP: Ocurrió un error crítico inesperado: {e}",
            exc_info=True,
        )
    finally:
        logger.info("Saliendo del hilo del cliente STOMP.")


@asynccontextmanager
async def lifespan(app_instance: app):
   
    # --- Inicio ---
    global stomp_thread, stomp_client
    logger.info("Servicio SISBI iniciándose...")
    logger.info("Iniciando el cliente STOMP en un hilo en segundo plano...")

    # Iniciar el cliente STOMP en un hilo separado
    stomp_thread = threading.Thread(target=run_stomp_client, daemon=True)
    stomp_thread.start()

    time.sleep(2)  # Dar tiempo al hilo para inicializar el cliente

    # Inyecta la instancia del cliente en el estado de la aplicación FastAPI
    if stomp_client:
        app_instance.state.stomp_client = stomp_client
        logger.info("Instancia del cliente STOMP inyectada en el estado de la aplicación FastAPI.")
    else:
        logger.error(
            "La instancia del cliente STOMP no estuvo disponible después de iniciar el hilo. La API podría fallar."
        )

    yield  # Aquí la API se ejecuta

    # --- Apagado ---
    logger.info("Servicio SISBI apagándose...")
    logger.info("Señalizando al hilo del cliente STOMP para detenerse...")
    shutdown_event.set()  # Señalar al hilo STOMP para detener su ciclo

    if stomp_thread and stomp_thread.is_alive():
        logger.info("Esperando que el hilo del cliente STOMP termine...")
        stomp_thread.join(timeout=5)  # Esperar máximo 5 segundos para que el hilo termine
        if stomp_thread.is_alive():
            logger.warning("El hilo del cliente STOMP no terminó de manera correcta.")

    logger.info("Apagado completo.")


# Asignar el manejador de vida útil a la aplicación
app.router.lifespan_context = lifespan


def main():
    logger.info("--- Iniciando el Servicio SISBI (API + Listener) ---")
    logger.info(f"Registro en: {config.SISBI_LOG_FILE}")
    logger.info(f"Conectando a ActiveMQ en {config.HOST}:{config.PORT}")
    logger.info(f"Escuchando en la cola: {config.SISBI_QUEUE}")
    logger.info(f"La API estará disponible en http://{config.API_HOST}:{config.API_PORT}")


    try:
        # Ejecutar el servidor Uvicorn programáticamente
        uvicorn.run(
            app,  # Usar la instancia de la aplicación con la configuración de lifespan
            host=config.API_HOST,
            port=config.API_PORT,
            log_level="info",  # Nivel de log de Uvicorn
        )
    except Exception as e:
        logger.critical(f"No se pudo ejecutar el servidor Uvicorn: {e}", exc_info=True)
        # Asegurar que el hilo STOMP se detenga si Uvicorn no pudo iniciarse
        shutdown_event.set()
        if stomp_thread and stomp_thread.is_alive():
            stomp_thread.join(timeout=2)
        sys.exit(1)
    finally:
        # Esto podría no alcanzarse si Uvicorn sale inesperadamente
        logger.info("--- Servicio SISBI Detenido ---")


if __name__ == "__main__":
    main()

