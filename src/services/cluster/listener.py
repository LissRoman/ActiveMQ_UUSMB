import logging
import time
import json
from ...utils.stomp_client import BaseListener
from ... import config, constants
from ...utils.formatter import format_console_message, format_dict_for_console

from . import db  # Importar el módulo de base de datos
from ...database import persist 

logger = logging.getLogger(__name__)  # Configura el logger para registrar los mensajes.

class ClusterListener(BaseListener):
    def __init__(self, client):
        super().__init__(client)
        # Inicializa la variable para identificar los mensajes procesados y evitar duplicados.
        self.processed_ids = set()
        self.preload_processed_ids()

    def process_message(self, message_data, headers):
        """Maneja los mensajes recibidos desde la cola de CLUSTER."""
        try:
            log_msg_fmt = format_dict_for_console(
                message_data, ["type", "source", "project_id", "retry"]
            )
            format_console_message(logging.INFO, "CLUSTER", f"Recibido: {log_msg_fmt}")
            logger.info(f"Mensaje recibido: {message_data}")

            if message_data.get("type") != constants.MESSAGE_TYPES["PROCESS"]:
                warn_msg = (
                    f"Ignorando mensaje con tipo distinto a PROCESS: {message_data.get('type')}"
                )
                logger.warning(warn_msg)
                format_console_message(logging.WARNING, "CLUSTER", warn_msg)
                return

            project_id = message_data.get("project_id")
            if not project_id:
                warn_msg = "El mensaje no contiene project_id. Se ignorará."
                logger.warning(warn_msg)
                format_console_message(logging.WARNING, "CLUSTER", warn_msg)
                return

            is_retry = message_data.get("retry", False)

            # Verifica si el mensaje es un duplicado o un reintento
            if db.is_project_processed(project_id):
                self.processed_ids.add(project_id)
                if not is_retry:
                    # Si no es un reintento y ya fue procesado, ignora el mensaje
                    warn_msg = f"Mensaje duplicado sin reintento para ID ya procesado: {project_id}. Se ignorará."
                    logger.warning(warn_msg)
                    format_console_message(logging.WARNING, "CLUSTER", warn_msg)

                    self.send_response(
                        project_id,
                        status=constants.PROCESS_STATUS["DUPLICATE_COMPLETED"],
                    )
                    return
                else:
                    # Si es un reintento, envía la respuesta de nuevo
                    info_msg = f"Reintento recibido para ID ya procesado: {project_id}. Enviando respuesta nuevamente."
                    logger.info(info_msg)
                    format_console_message(logging.INFO, "CLUSTER", info_msg)
                    self.send_cluster_response(project_id, message_data.get("data", {}))
                    return

            # Si es un nuevo mensaje, se procesa
            self.processed_ids.add(project_id)
            db.mark_project_as_processed(project_id,status=constants.PROCESS_STATUS["COMPLETED"])
            #self.simulate_cluster_processing(project_id, message_data.get("data", {}))
           
        except Exception as e:
            error_msg = f"Error al procesar el mensaje en ClusterListener: {e}"
            logger.error(error_msg, exc_info=True)
            format_console_message(logging.ERROR, "CLUSTER", error_msg)
            # En caso de error, se podría enviar un mensaje de error a la cola LISS

    def preload_processed_ids(self):
        try:
            with persist.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(f'SELECT project_id FROM "{db.SERVICE_NAME}"')
                rows = cur.fetchall()
                self.processed_ids = set(row[0] for row in rows)
            logger.info(f"IDs procesados precargados: {len(self.processed_ids)}")
        except Exception as e:
            logger.error(f"Error al cargar project_ids procesados: {e}")

    def simulate_cluster_processing(self, project_id, data_from_watson):
        """Simula la tarea de procesamiento del Cluster."""
        start_msg = f"Iniciando análisis simulado del cluster para ID: {project_id}"
        logger.info(start_msg)
        format_console_message("SERVICE", "CLUSTER", start_msg)

        # Simulación de procesamiento de análisis en 7 pasos
        for i in range(1, 8):
            progress_msg = f"Paso de análisis {i}/7 para {project_id}..."
            logger.debug(progress_msg)
            format_console_message(logging.DEBUG, "CLUSTER", progress_msg)
            time.sleep(0.7)  # Simula trabajo

        completion_msg = f"Análisis simulado del cluster completado para ID: {project_id}"
        logger.info(completion_msg)
        format_console_message("SERVICE", "CLUSTER", completion_msg)
        self.send_cluster_response(
            project_id, {"analysis_results": "sample_data"}
        )

    def send_cluster_response(self, project_id, results_data):
        """Envía un mensaje de tipo RESPONSE de vuelta a LISS."""
        response_payload = {
            "type": constants.MESSAGE_TYPES["RESPONSE"],
            "source": constants.SERVICES["CLUSTER"],
            "destination": constants.SERVICES["LISS"],
            "project_id": project_id,
            "timestamp": int(time.time()),
            "status": constants.PROCESS_STATUS["COMPLETED"],
            "data": {
                "process_info": "Análisis del cluster completado exitosamente (simulado)",
                "job_id": f"cluster-job-{project_id}-{int(time.time())}",
                "results": results_data,
            },
        }
        log_msg_fmt = format_dict_for_console(
            response_payload, ["type", "destination", "project_id", "status", "source"]
        )
        info_msg = f"Enviando respuesta a LISS: {log_msg_fmt}"
        logger.info(f"Respuesta enviada: {response_payload}")
        format_console_message(logging.INFO, "CLUSTER", info_msg)

        # Envía la respuesta a la cola LISS
        if not self.client.send(config.LISS_QUEUE, response_payload):
            error_msg = f"No se pudo enviar la respuesta para {project_id} a la cola de LISS."
            logger.error(error_msg)
            format_console_message(logging.ERROR, "CLUSTER", error_msg)
            # Maneja el error de envío.
