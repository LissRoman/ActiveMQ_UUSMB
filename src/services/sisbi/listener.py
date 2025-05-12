# src/services/sisbi/listener.py
import logging
import json
from ...utils.stomp_client import BaseListener
from ... import config, constants
from ...utils.formatter import format_console_message, format_dict_for_console

logger = logging.getLogger(__name__)  # Obtener el logger configurado en main.py


class SISBIListener(BaseListener):
    def __init__(self, client):
        super().__init__(client)
        # Aquí podrías agregar estado si SISBI necesita rastrear simulaciones en curso

    def process_message(self, message_data, headers):
        """Maneja los mensajes recibidos en la cola de SISBI (por ejemplo, actualizaciones de estado)."""
        try:
            log_msg_fmt = format_dict_for_console(
                message_data,
                ["type", "source", "project_id", "status", "service", "message"],
            )
            format_console_message(
                logging.INFO, "SISBI", f"Mensaje recibido con estado/respuesta: {log_msg_fmt}"
            )
            logger.info(f"Mensaje recibido en la cola SISBI: {message_data}")

            msg_type = message_data.get("type")
            source = message_data.get("source")
            project_id = message_data.get("project_id")
            status = message_data.get("status")
            service = message_data.get("service")
            message_text = message_data.get("message", "")

            if (
                msg_type == constants.MESSAGE_TYPES["STATUS"]
                and source == constants.SERVICES["LISS"]
            ):
                # Registrar la actualización de estado desde LISS
                log_level = logging.INFO
                if status in [
                    constants.PROCESS_STATUS["FAILED"],
                    constants.PROCESS_STATUS["TIMEOUT"],
                ]:
                    log_level = logging.ERROR
                    format_console_message(
                        logging.ERROR,
                        "SISBI",
                        f"Error de estado de LISS para {project_id} ({service}): {status} - {message_text}",
                    )
                elif (
                    status == constants.PROCESS_STATUS["COMPLETED"]
                    and service == constants.SERVICES["CLUSTER"]
                ):
                    # Registrar la finalización
                    format_console_message(
                        logging.INFO,
                        "SISBI",
                        f"Flujo de trabajo COMPLETADO para {project_id}. Último paso: {service}. Mensaje: {message_text}",
                    )
                else:
                    # Registrar pasos intermedios de finalización
                    format_console_message(
                        logging.INFO,
                        "SISBI",
                        f"Actualización de estado de LISS para {project_id} ({service}): {status} - {message_text}",
                    )

                logger.log(
                    log_level,
                    f"Estado de LISS: project={project_id}, service={service}, status={status}, msg={message_text}",
                )
                
            else:
                logger.debug(
                    f"Mensaje de tipo '{msg_type}' recibido de '{source}' en la cola SISBI. Ignorando."
                )

        except Exception as e:
            error_msg = f"Error al procesar el mensaje en SISBIListener: {e}"
            logger.error(error_msg, exc_info=True)
            format_console_message(logging.ERROR, "SISBI", error_msg)
