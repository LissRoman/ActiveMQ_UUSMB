# src/services/liss/listener.py
import logging
import time
import json
import threading
from collections import defaultdict
from ...utils.stomp_client import BaseListener
from ... import config, constants
from ...utils.formatter import format_console_message, format_dict_for_console

logger = logging.getLogger(__name__)

INITIAL_WAIT_TIME = 30  
RETRY_TIMEOUT = 5  


class LISSListener(BaseListener):
    def __init__(self, client):
        super().__init__(client)
        self.process_state = defaultdict(lambda: defaultdict(lambda: None))
        self.timers = defaultdict(lambda: defaultdict(lambda: None))

    def _cleanup_timers(self, project_id, service_name):
        """Cancela los temporizadores para un paso específico del proceso."""
        if project_id in self.timers and service_name in self.timers[project_id]:
            timer = self.timers[project_id].get(service_name)
            if timer:
                logger.debug(f"Cancelando temporizador para {project_id} -> {service_name}")
                timer.cancel()
            # Eliminar la entrada del temporizador de todas formas, para limpiar el estado
            if service_name in self.timers[project_id]:
                del self.timers[project_id][service_name]
            if not self.timers[project_id]:  # Eliminar project_id si ya no tiene temporizadores
                del self.timers[project_id]

    def _send_request_with_retry(
        self, target_service_name, target_queue, message_payload, project_id
    ):
        """Envía un mensaje, establece el estado y arranca el temporizador inicial de espera."""
        log_msg_fmt = format_dict_for_console(
            message_payload, ["type", "destination", "project_id"]
        )
        info_msg = f"Enviando solicitud a {target_service_name}: {log_msg_fmt}"
        logger.info(f"Enviando solicitud: {message_payload}")
        format_console_message(logging.INFO, "LISS", info_msg)

        # Asegura que el payload sea un string JSON antes de enviarlo
        try:
            payload_str = json.dumps(message_payload)
        except TypeError:
            logger.error(
                f"No se pudo serializar el payload a JSON para {project_id} -> {target_service_name}"
            )
            format_console_message(
                logging.ERROR,
                "LISS",
                f"No se pudo serializar el payload para {project_id} -> {target_service_name}",
            )
            self.process_state[project_id][target_service_name] = "failed"
            self._cleanup_timers(project_id, target_service_name)
            return

        if self.client.send(target_queue, payload_str):
            self.process_state[project_id][target_service_name] = "waiting"
            # Inicia el temporizador de espera inicial
            timer = threading.Timer(
                INITIAL_WAIT_TIME,
                self._attempt_retry,
                args=[target_service_name, target_queue, message_payload, project_id],
            )
            self.timers[project_id][target_service_name] = timer
            timer.start()
            logger.debug(
                f"Temporizador inicial iniciado ({INITIAL_WAIT_TIME}s) para {project_id} -> {target_service_name}"
            )
        else:
            error_msg = (
                f"El envío inicial a {target_service_name} para {project_id} falló."
            )
            logger.error(error_msg)
            format_console_message(logging.ERROR, "LISS", error_msg)
            # Si el envío inicial falla, marcar como fallido y limpiar temporizadores
            self.process_state[project_id][target_service_name] = "failed"
            self._cleanup_timers(project_id, target_service_name)

    def _attempt_retry(
        self, target_service_name, target_queue, original_payload, project_id
    ):
        """Llamado por el primer temporizador. Envía un mensaje de reintento y arranca el temporizador final."""
        # Verifica el estado *antes* de intentar el reintento
        current_state = self.process_state[project_id].get(target_service_name)
        if current_state != "waiting":
            logger.info(
                f"Intento de reintento para {project_id} -> {target_service_name} omitido (estado es {current_state})."
            )
            # Si el estado no es 'waiting', significa que ya se recibió una respuesta o se manejó el timeout
            self._cleanup_timers(
                project_id, target_service_name
            )  # Asegura que el temporizador se cancele
            return

        warn_msg = f"No hay respuesta de {target_service_name} para {project_id} después de {INITIAL_WAIT_TIME}s. Reintentando..."
        logger.warning(warn_msg)
        format_console_message(logging.WARNING, "LISS", warn_msg)

        # Modifica el payload para reintento
        retry_payload = original_payload.copy()
        retry_payload["retry"] = True
        retry_payload["timestamp"] = int(time.time())  # Actualiza timestamp

        # Calcula la cabecera de expiración (por ejemplo, RETRY_TIMEOUT + buffer)
        headers = {
            "expires": str(int(time.time() * 1000) + (RETRY_TIMEOUT * 1000) + 1000)
        }  # Añade 1s de buffer

        # Asegura que el payload sea un string JSON para enviarlo
        try:
            payload_str = json.dumps(retry_payload)
        except TypeError:
            logger.error(
                f"No se pudo serializar el payload de reintento a JSON para {project_id} -> {target_service_name}"
            )
            format_console_message(
                logging.ERROR,
                "LISS",
                f"No se pudo serializar el payload de reintento para {project_id} -> {target_service_name}",
            )
            self.process_state[project_id][target_service_name] = "failed"
            self._cleanup_timers(project_id, target_service_name)
            return

        if self.client.send(
            target_queue, payload_str, headers=headers
        ):  # Asegura que el payload sea un string JSON
            self.process_state[project_id][target_service_name] = "retry_sent"
            # Inicia el segundo temporizador para el timeout final
            final_timer = threading.Timer(
                RETRY_TIMEOUT,
                self._notify_timeout,
                args=[target_service_name, project_id],
            )
            self.timers[project_id][target_service_name] = final_timer
            final_timer.start()
            logger.debug(
                f"Reintento enviado. Temporizador final iniciado ({RETRY_TIMEOUT}s) para {project_id} -> {target_service_name}"
            )
        else:
            error_msg = f"El envío del reintento a {target_service_name} para {project_id} falló."
            logger.error(error_msg)
            format_console_message(logging.ERROR, "LISS", error_msg)
            # Si el reintento falla, marcar como fallido y limpiar temporizadores
            self.process_state[project_id][target_service_name] = "failed"
            self._cleanup_timers(project_id, target_service_name)

    def _notify_timeout(self, target_service_name, project_id):
        """Llamado por el segundo temporizador si no se recibe respuesta después del reintento."""
        # Verifica el estado *antes* de notificar timeout
        current_state = self.process_state[project_id].get(target_service_name)
        if current_state != "retry_sent":
            logger.info(
                f"Notificación de timeout final para {project_id} -> {target_service_name} omitida (estado es {current_state})."
            )
            # Si el estado no es 'retry_sent', significa que se recibió respuesta o cambió el estado
            self._cleanup_timers(
                project_id, target_service_name
            )  # Asegura que el temporizador se cancele
            return

        error_msg = f"Timeout: No hay respuesta de {target_service_name} para {project_id} después del reintento."
        logger.error(error_msg)
        format_console_message(logging.ERROR, "LISS", error_msg)

        # Marca como timeout y limpia temporizadores
        self.process_state[project_id][target_service_name] = "timeout"
        self._cleanup_timers(project_id, target_service_name)
        self._final_project_cleanup(project_id)

    def process_message(self, message_data, headers):
        """Maneja los mensajes recibidos en la cola de LISS."""
        # Asegura que message_data sea un dict (stomp.py normalmente convierte JSON)
        if isinstance(message_data, str):
            try:
                message_data = json.loads(message_data)
            except json.JSONDecodeError:
                logger.error(f"No se pudo decodificar el mensaje JSON: {message_data}")
                format_console_message(
                    logging.ERROR, "LISS", "No se pudo decodificar el mensaje JSON"
                )
                return
        elif not isinstance(message_data, dict):
            logger.error(f"Formato de mensaje inesperado recibido: {type(message_data)}")
            format_console_message(
                logging.ERROR, "LISS", "Formato de mensaje inesperado recibido"
            )
            return

        try:
            log_msg_fmt = format_dict_for_console(
                message_data,
                [
                    "type",
                    "source",
                    "project_id",
                    "status",
                ],  
            )
            format_console_message(logging.INFO, "LISS", f"Recibido: {log_msg_fmt}")
            logger.info(f"Mensaje recibido: {message_data}")

            msg_type = message_data.get("type")
            source = message_data.get("source")
            project_id = message_data.get("project_id")
            message_status = message_data.get("status")  # Obtener el campo status

            if not project_id:
                logger.warning("Ignorando mensaje sin project_id.")
                return

            # --- Lógica de orquestación ---

            # 1. Mensaje de SISBI para iniciar el proceso
            if (
                msg_type == constants.MESSAGE_TYPES["START"]
                and source == constants.SERVICES["SISBI"]
            ):
                # Verifica si *algún* paso para este project_id está activo (waiting, retry_sent)
                active_steps = [
                    s
                    for s, state in self.process_state[project_id].items()
                    if state in ["waiting", "retry_sent"]
                ]

                if active_steps:
                    warn_msg = f"Ignorando START para {project_id}, pasos {active_steps} ya están en progreso."
                    logger.warning(warn_msg)
                    format_console_message(logging.WARNING, "LISS", warn_msg)
                    return

                # Preparar mensaje para Watson
                watson_payload = {
                    "type": constants.MESSAGE_TYPES["PROCESS"],
                    "source": constants.SERVICES["LISS"],
                    "destination": constants.SERVICES["WATSON"],
                    "project_id": project_id,
                    "timestamp": int(time.time()),
                    "message": message_data.get(
                        "message", ""
                    ),  # Pasar mensaje original
                }
                self._send_request_with_retry(
                    constants.SERVICES["WATSON"],
                    config.WATSON_QUEUE,
                    watson_payload,
                    project_id,
                )

# 2. Respuesta de WATSON
            elif (
                msg_type == constants.MESSAGE_TYPES["RESPONSE"]
                and source == constants.SERVICES["WATSON"]
            ):
                current_watson_state = self.process_state[project_id].get(
                    constants.SERVICES["WATSON"]
                )
                # Solo procesar la respuesta si estábamos esperando o reintentando el envío para Watson
                if current_watson_state in ["waiting", "retry_sent"]:
                    # --- Manejar el estado de la respuesta de Watson ---
                    if message_status == constants.PROCESS_STATUS["COMPLETED"]:
                        info_msg = (
                            f"Respuesta de Watson (COMPLETED) recibida para {project_id}."
                        )
                        logger.info(info_msg)
                        format_console_message(logging.INFO, "LISS", info_msg)

                        # Marcar el paso de Watson como completado y limpiar sus temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["WATSON"]
                        ] = "completed"
                        self._cleanup_timers(project_id, constants.SERVICES["WATSON"])

                        # Proceder al siguiente paso: Cluster
                        # Pasar los datos de la respuesta de Watson al siguiente paso
                        self._initiate_cluster_step(
                            project_id, message_data.get("data", {})
                        )

                    elif (
                        message_status
                        == constants.PROCESS_STATUS["DUPLICATE_COMPLETED"]
                    ):
                        info_msg = f"Respuesta de Watson (DUPLICATE_COMPLETED) recibida para {project_id}. Tarea previamente completada en el lado de Watson. OMITIENDO paso de Cluster."
                        logger.info(info_msg)
                        format_console_message(logging.INFO, "LISS", info_msg)

                        # Marcar el paso de Watson como completado (desde la perspectiva de orquestación de LISS) y limpiar temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["WATSON"]
                        ] = "completed"
                        self._cleanup_timers(project_id, constants.SERVICES["WATSON"])
                        self._final_project_cleanup(project_id)

                    elif message_status == constants.PROCESS_STATUS["FAILED"]:
                        error_msg = f"Respuesta de Watson (FAILED) recibida para {project_id}. Orquestación detenida para este proyecto."
                        logger.error(error_msg)
                        format_console_message(logging.ERROR, "LISS", error_msg)

                        # Marcar el paso de Watson como fallido y limpiar temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["WATSON"]
                        ] = "failed"
                        self._cleanup_timers(project_id, constants.SERVICES["WATSON"])
                        self._final_project_cleanup(project_id)

                    else:
                        # Manejar un estado inesperado de Watson
                        error_msg = f"Estado inesperado '{message_status}' recibido de Watson para {project_id}. Marcando paso como fallido."
                        logger.error(error_msg)
                        format_console_message(logging.ERROR, "LISS", error_msg)

                        # Marcar el paso de Watson como fallido debido al estado inesperado y limpiar temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["WATSON"]
                        ] = "failed"
                        self._cleanup_timers(project_id, constants.SERVICES["WATSON"])
                        self._final_project_cleanup(project_id)

                else:
                    # Ignorar respuesta tardía/inesperada si LISS no estaba esperando a Watson
                    logger.warning(
                        f"Ignorando respuesta tardía/inesperada de Watson para {project_id} (estado: {current_watson_state}). El estado fue: {message_status}"
                    )

            # 3. Respuesta de CLUSTER
            elif (
                msg_type == constants.MESSAGE_TYPES["RESPONSE"]
                and source == constants.SERVICES["CLUSTER"]
            ):
                current_cluster_state = self.process_state[project_id].get(
                    constants.SERVICES["CLUSTER"]
                )
                # Solo procesar la respuesta si estábamos esperando o reintentando el envío para Cluster
                if current_cluster_state in ["waiting", "retry_sent"]:
                    # --- Manejar el estado de la respuesta de Cluster ---
                    if message_status == constants.PROCESS_STATUS["COMPLETED"]:
                        info_msg = f"Respuesta de Cluster (COMPLETED) recibida para {project_id}. Proyecto completado."
                        logger.info(info_msg)
                        format_console_message(logging.INFO, "LISS", info_msg)

                        # Marcar el paso de Cluster como completado y limpiar sus temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["CLUSTER"]
                        ] = "completed"
                        self._cleanup_timers(project_id, constants.SERVICES["CLUSTER"])

                        # Limpieza final para el estado completo del proyecto
                        self._final_project_cleanup(project_id)

                    elif (
                        message_status
                        == constants.PROCESS_STATUS["DUPLICATE_COMPLETED"]
                    ):
                        info_msg = f"Respuesta de Cluster (DUPLICATE_COMPLETED) recibida para {project_id}. Tarea previamente completada en el lado de Cluster. Proyecto completado."
                        logger.info(info_msg)
                        format_console_message(logging.INFO, "LISS", info_msg)

                        # Marca el paso de Cluster como completado (desde la perspectiva de orquestación de LISS) y limpiar temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["CLUSTER"]
                        ] = "completed"
                        self._cleanup_timers(project_id, constants.SERVICES["CLUSTER"])

                        # Limpieza final
                        self._final_project_cleanup(project_id)

                    elif message_status == constants.PROCESS_STATUS["FAILED"]:
                        error_msg = f"Respuesta de Cluster (FAILED) recibida para {project_id}. Orquestación detenida para este proyecto."
                        logger.error(error_msg)
                        format_console_message(logging.ERROR, "LISS", error_msg)

                        # Marcar el paso de Cluster como fallido y limpiar temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["CLUSTER"]
                        ] = "failed"
                        self._cleanup_timers(project_id, constants.SERVICES["CLUSTER"])
                        self._final_project_cleanup(project_id)

                    else:
                        # Manejar un estado inesperado de Cluster
                        error_msg = f"Estado inesperado '{message_status}' recibido de Cluster para {project_id}. Marcando paso como fallido."
                        logger.error(error_msg)
                        format_console_message(logging.ERROR, "LISS", error_msg)

                        # Marcar el paso de Cluster como fallido debido al estado inesperado y limpiar temporizadores
                        self.process_state[project_id][
                            constants.SERVICES["CLUSTER"]
                        ] = "failed"
                        self._cleanup_timers(project_id, constants.SERVICES["CLUSTER"])
                        self._final_project_cleanup(project_id)

                else:
                    # Ignorar respuesta tardía/inesperada si LISS no estaba esperando a Cluster
                    logger.warning(
                        f"Ignorando respuesta tardía/inesperada de Cluster para {project_id} (estado: {current_cluster_state}). El estado fue: {message_status}"
                    )

            # Manejar otros tipos de mensajes o fuentes si es necesario
            else:
                logger.debug(
                    f"Mensaje de tipo '{msg_type}' recibido de la fuente '{source}' para el proyecto '{project_id}' no manejado."
                )

        except Exception as e:
            error_msg = f"Error al procesar el mensaje en LISSListener: {e}"
            logger.error(error_msg, exc_info=True)
            format_console_message(logging.ERROR, "LISS", error_msg)
            # Intento básico de limpieza en caso de excepción durante el procesamiento
            project_id = message_data.get("project_id", "unknown")
            source = message_data.get("source", "unknown")
            if project_id != "unknown":
                # Intentar limpiar temporizadores para el paso conocido de la fuente si es posible
                if source in [
                    constants.SERVICES["WATSON"],
                    constants.SERVICES["CLUSTER"],
                ]:
                    self._cleanup_timers(project_id, source)
                    # Opcionalmente, marcar el paso como fallido
                    self.process_state[project_id][source] = "failed"

    # --- Métodos auxiliares para el flujo de orquestación ---

    def _initiate_cluster_step(self, project_id, data_from_watson):
        """Verifica el estado de Cluster y envía un mensaje a Cluster si está listo."""
        # Este método solo se llama cuando Watson ha respondido explícitamente con COMPLETED (no DUPLICATE_COMPLETED)
        # Aún verificamos el estado de Cluster de manera defensiva en caso de que una respuesta duplicada de Watson
        # que no detectamos haya causado que Cluster ya comenzara.
        current_cluster_state = self.process_state[project_id].get(
            constants.SERVICES["CLUSTER"]
        )

        # Si el estado de Cluster está actualmente activo (esperando/reintentando) o ya completado/fallido/timeout,
        # NO enviar el mensaje nuevamente.
        # Solo enviar si el estado es None (lo que significa que el paso aún no se ha iniciado)
        if current_cluster_state is None:
            logger.info(
                f"Iniciando el paso de Cluster para {project_id} después de que Watson COMPLETED."
            )
            cluster_payload = {
                "type": constants.MESSAGE_TYPES["PROCESS"],
                "source": constants.SERVICES["LISS"],
                "destination": constants.SERVICES["CLUSTER"],
                "project_id": project_id,
                "timestamp": int(time.time()),
                "data": data_from_watson,  # Pasar los datos de la respuesta de Watson
            }
            self._send_request_with_retry(
                constants.SERVICES["CLUSTER"],
                config.CLUSTER_QUEUE,
                cluster_payload,
                project_id,
            )
        else:
            # Este caso idealmente no debería suceder si el estado de LISS es consistente,
            # pero lo registra si el estado de Cluster es inesperado cuando Watson COMPLETED.
            warn_msg = f"Omitiendo la iniciación del paso de Cluster para {project_id} después de Watson COMPLETED, ya que el estado de Cluster es inesperadamente '{current_cluster_state}'."
            logger.warning(warn_msg)
            format_console_message(logging.WARNING, "LISS", warn_msg)

    def _final_project_cleanup(self, project_id):
        """Verifica si todos los pasos para un proyecto están completados/finalizados y limpia el estado general del proyecto."""
        project_steps = self.process_state.get(project_id)
        if not project_steps:
            # El estado de este project_id ya está vacío, no hay nada que hacer
            return

        # Definir qué estados se consideran "finales" para un paso
        final_states = ["completed", "failed", "timeout", None]

        # Suponiendo que Watson y Cluster son los pasos principales que LISS rastrea para un proyecto
        watson_state = project_steps.get(constants.SERVICES["WATSON"])
        cluster_state = project_steps.get(constants.SERVICES["CLUSTER"])

        all_steps_final = all(state in final_states for state in project_steps.values())

        if all_steps_final:
            logger.info(
                f"Todos los pasos rastreados alcanzaron estados finales para el proyecto {project_id}. Limpieza del estado final."
            )
            # Eliminar la entrada del estado del proyecto
            del self.process_state[project_id]
            # Asegurarse de que los temporizadores también se limpien para este project_id (debería ser a través de _cleanup_timers, pero verificar dos veces)
            if project_id in self.timers:
                for service_name in list(
                    self.timers[project_id].keys()
                ):  # Iterar sobre una copia
                    self._cleanup_timers(project_id, service_name)
                if not self.timers[project_id]:
                    del self.timers[project_id]