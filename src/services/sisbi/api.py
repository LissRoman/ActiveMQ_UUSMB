# src/services/sisbi/api.py
import logging
import time
import json
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field  # Usar Field para valores por defecto
from ... import config, constants
from ...utils.formatter import format_console_message, format_dict_for_console

logger = logging.getLogger(__name__)


# Modelo de cuerpo de solicitud para el endpoint de simulación
class MessagePayload(BaseModel):
    message: str = Field(default=constants.DEFAULT_MESSAGE)
    project_id: str = Field(default=constants.DEFAULT_PROJECT_ID)


#instancia de la aplicación FastAPI
app = FastAPI(title="API del Servicio SISBI")


@app.on_event("startup")
async def startup_event():
    logger.info("Inicio de la aplicación FastAPI...")


@app.on_event("shutdown")
def shutdown_event():
    # Esto se llama cuando Uvicorn se apaga
    logger.info("Apagado de la aplicación FastAPI...")
    # La desconexión del cliente STOMP debe manejarse en la lógica de apagado de main.py


@app.get("/health")
async def health_check():
    """Endpoint de comprobación básica de salud."""
    logger.debug("Se ha llamado al endpoint de comprobación de salud.")
    stomp_client = app.state.stomp_client  # Acceder al cliente inyectado
    if stomp_client and stomp_client.is_connected():
        return {"status": "OK", "message_broker_connection": "Conectado"}
    else:
        logger.warning("Comprobación de salud: La conexión con el broker de mensajes está caída.")
        return {"status": "OK", "message_broker_connection": "Desconectado"}
    
    
@app.post("/simulacion")
async def simulacion_endpoint(payload: MessagePayload, request: Request):
    """Endpoint para activar un nuevo flujo de trabajo de simulación."""
    logger.info(f"Se recibió una solicitud de simulación para project_id: {payload.project_id}")
    format_console_message(
        logging.INFO,
        "SISBI",
        f"Solicitud de simulación: project={payload.project_id}, msg='{payload.message}'",
    )

    # Acceder a la instancia del cliente STOMP inyectada en el estado de la aplicación
    stomp_client = request.app.state.stomp_client

    if not stomp_client or not stomp_client.is_connected():
        error_msg = "No se puede iniciar la simulación: El broker de mensajes no está conectado."
        logger.error(error_msg)
        format_console_message(logging.ERROR, "SISBI", error_msg)
        raise HTTPException(status_code=503, detail=error_msg)  # Servicio no disponible

    # Construir el mensaje inicial para LISS
    start_message = {
        "type": constants.MESSAGE_TYPES["START"],
        "source": constants.SERVICES["SISBI"],
        "destination": constants.SERVICES["LISS"],  # Apuntando a LISS
        "project_id": payload.project_id,
        "timestamp": int(time.time()),
        "message": payload.message,
    }

    log_msg_fmt = format_dict_for_console(
        start_message, ["type", "destination", "project_id"]
    )
    info_msg = f"Enviando START a LISS: {log_msg_fmt}"
    logger.info(f"Enviando mensaje START: {start_message}")
    format_console_message(logging.INFO, "SISBI", info_msg)

    # Enviar el mensaje usando el cliente inyectado
    success = stomp_client.send(config.LISS_QUEUE, start_message)

    if success:
        response_text = (
            f"Simulación iniciada con éxito para project_id: {payload.project_id}"
        )
        logger.info(response_text)
        return {
            "status": "OK",
            "message": "Mensaje de inicio de simulación enviado a LISS.",
            "project_id": payload.project_id,
        }
    else:
        error_msg = f"Falló el envío del mensaje de inicio de simulación para project_id: {payload.project_id}"
        logger.error(error_msg)
        format_console_message(logging.ERROR, "SISBI", error_msg)
        raise HTTPException(
            status_code=500, detail="Falló el envío del mensaje al motor de flujo de trabajo."
        )
