# src/services/watson/listener.py
import logging
import threading
import time
import json
from ...utils.stomp_client import BaseListener
from ... import config, constants
from ...utils.formatter import format_console_message, format_dict_for_console

import subprocess
from . import  db  # Importar el módulo de base de datos
from ...database import persist 

logger = logging.getLogger(__name__)  # Obtener el logger configurado en main.py


class WatsonListener(BaseListener):
    def __init__(self, client):
        super().__init__(client)
        self.processed_ids = set()
        self.preload_processed_ids()
       

    def process_message(self, message_data, headers):
        """Maneja los mensajes recibidos en la cola WATSON."""
        try:
            log_msg_fmt = format_dict_for_console(
                message_data, ["type", "source", "project_id", "retry"]
            )
            format_console_message(logging.INFO, "WATSON", f"Recibido: {log_msg_fmt}")
            logger.info(f"Mensaje recibido: {message_data}")

            if message_data.get("type") != constants.MESSAGE_TYPES["PROCESS"]:
                warn_msg = (
                    f"Ignorando tipo de mensaje no-PROCESS: {message_data.get('type')}"
                )
                logger.warning(warn_msg)
                format_console_message(logging.WARNING, "WATSON", warn_msg)
                return

            project_id = message_data.get("project_id")
            if not project_id:
                logger.error("Mensaje recibido sin project_id. Ignorando.")
                return
            is_retry = message_data.get("retry", False)

            # Verificar si este project_id ya fue procesado antes
            if db.is_project_processed(project_id):
                self.processed_ids.add(project_id)
                if not is_retry:
                    # Caso 1: Mensaje duplicado (no es un reintento) para un ID ya procesado.
                    # Enviar una respuesta a LISS indicando que ya está procesado.
                    warn_msg = f"Mensaje duplicado no-reintento para ID ya procesado: {project_id}. Enviando respuesta 'ya procesado'."
                    logger.warning(warn_msg)
                    format_console_message(logging.WARNING, "WATSON", warn_msg)
                    # Enviar respuesta indicando que ya fue procesado
                    self.send_response(
                        project_id,
                        status=constants.PROCESS_STATUS["DUPLICATE_COMPLETED"],
                    )
                    return
                
                else:
                    # Caso 2: Es un reintento para un ID ya procesado.
                    # Enviar la respuesta nuevamente, también indicando que ya fue procesado.
                    info_msg = f"Recibido reintento para ID ya procesado: {project_id}. Enviando respuesta nuevamente (ya procesado)."
                    logger.info(info_msg)
                    format_console_message(logging.INFO, "WATSON", info_msg)
                    # Enviar respuesta indicando que ya fue procesado debido al reintento
                    self.send_response(project_id, message_data.get("data",{}))
                    return

            # Caso 3: Nuevo mensaje (no es un duplicado en processed_ids) o un reintento
            # para un ID que de alguna manera no estaba en processed_ids (menos probable si el estado es persistente).
            self.processed_ids.add(project_id)
            self.simulate_process(project_id)


        except Exception as e:
            error_msg = f"Error procesando el mensaje en WatsonListener: {e}"
            logger.error(error_msg, exc_info=True)
            format_console_message(logging.ERROR, "WATSON", error_msg)

    #Metodo para los id duplicados (new) 
    def preload_processed_ids(self):
        
        try:
            with persist.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(f'SELECT project_id FROM "{db.SERVICE_NAME}"')
                rows = cur.fetchall()
                self.processed_ids = set(row[0] for row in rows)
            logger.info(f"Preprocesados cargados: {len(self.processed_ids)} IDs")
        except Exception as e:
            logger.error(f"Error cargando project_ids procesados: {e}")



    def simulate_process(self, project_id):
        """Simula la tarea de procesamiento de Watson."""
       
        
        try:
            bash_script = "/home/liss/script-liss.sh"  # Ruta del script .sh
            result = subprocess.run(
                ["bash",bash_script],
                capture_output=True,
                text=True,
                check=True)

            print("Command executed successfully")
            print("Output:", result.stdout)
            self.send_response(project_id, status=constants.PROCESS_STATUS["COMPLETED"])  
            db.mark_project_as_processed(project_id,status=constants.PROCESS_STATUS["COMPLETED"]) 

        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with error code: {e.returncode}")
            self.send_response(project_id, status=constants.PROCESS_STATUS["FAILED"])



    def send_response(self, project_id, status):
        """Envía un mensaje de RESPUESTA de vuelta a LISS con un estado específico."""

        data_payload = {
            "process_info": "Proceso de Watson completado (simulado)",
            "job_id": f"watson-job-{project_id}-{int(time.time())}",
        }

        # Ajusta los datos según el estado que se está enviando
        if status == constants.PROCESS_STATUS["DUPLICATE_COMPLETED"]:
            data_payload["already_processed"] = True
            data_payload["process_info"] = (
                "Watson reportó: La tarea ya fue completada para este ID."
            )
        elif status == constants.PROCESS_STATUS["COMPLETED"]:
            # Información estándar de finalización
            data_payload["process_info"] = "Clonación completada exitosamente (simulada)."
            pass  # No se necesitan banderas especiales para la finalización estándar

        response_payload = {
            "type": constants.MESSAGE_TYPES["RESPONSE"],
            "source": constants.SERVICES["WATSON"],
            "destination": constants.SERVICES["LISS"],
            "project_id": project_id,
            "timestamp": int(time.time()),
            "status": status,  # Usar el estado pasado a la función
            "data": data_payload,  # Incluir los datos relevantes según el estado
        }

        log_msg_fmt = format_dict_for_console(
            response_payload,
            ["type", "destination", "project_id", "status", "data.already_processed"],
        )
        info_msg = f"Enviando respuesta a LISS con estado {status}: {log_msg_fmt}"
        logger.info(f"Enviando respuesta: {response_payload}")
        format_console_message(logging.INFO, "WATSON", info_msg)

        # Enviar la respuesta de vuelta a la cola LISS
        if not self.client.send(config.LISS_QUEUE, response_payload):
            error_msg = f"Falló el envío de la respuesta para {project_id} a la cola LISS."
            logger.error(error_msg)
            format_console_message(logging.ERROR, "WATSON", error_msg)