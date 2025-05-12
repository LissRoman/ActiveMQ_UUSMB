# src/utils/stomp_client.py
import stomp
import time
import json
import logging
from .. import config  # Importación relativa desde el paquete

# Obtener una instancia de logger (puede ser configurada externamente)
logger = logging.getLogger(__name__) 

class BaseListener(stomp.ConnectionListener):
    """Listener base con manejo común de errores."""
    def __init__(self, client):
        self.client = client  # Referencia al cliente STOMP

    def on_error(self, frame):
        """Manejo por defecto de errores STOMP."""
        error_msg = f"Error STOMP recibido: {frame.body}"
        logger.error(error_msg)

    def on_disconnected(self):
        """Llamado por stomp.py cuando se pierde la conexión."""
        logger.warning("Conexión STOMP perdida.")
        self.client.schedule_reconnect()

    def on_message(self, frame):
        logger.debug(f"Mensaje crudo recibido en BaseListener: {frame.body}")
        try:
            message_data = json.loads(frame.body)
            self.process_message(message_data, frame.headers)
        except json.JSONDecodeError:
            logger.error(f"No se pudo decodificar JSON: {frame.body}")
        except Exception as e:
            logger.error(f"Error procesando mensaje en BaseListener: {e}", exc_info=True)

    def process_message(self, message_data, headers):
        """Procesamiento de mensajes parseados. Debe ser implementado."""
        raise NotImplementedError("Subclases deben implementar process_message")


class StompClient:
    """Maneja conexión STOMP, reconexión automática y envío de mensajes."""
    def __init__(self, host, port, username, password, listener_instance=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.conn = None
        self.listener = listener_instance if listener_instance else BaseListener(self)
        if hasattr(self.listener, 'client') and self.listener.client is None:
            self.listener.client = self
        self._is_connected = False
        self._should_run = True
        self._reconnect_scheduled = False
        self._subscriptions = {}  # Almacena suscripciones: {dest: {id, ack}}

    def _connect_attempt(self):
        """Intenta establecer conexión STOMP."""
        try:
            logger.info(f"Intentando conectar a {self.host}:{self.port}...")
            self.conn = stomp.Connection([(self.host, self.port)],
                                         keepalive=True,
                                         heartbeats=(10000, 10000))  # 10s de latido
            self.conn.set_listener('', self.listener)
            self.conn.connect(self.username, self.password, wait=True)
            self._is_connected = True
            self._reconnect_scheduled = False
            logger.info("Conexión STOMP establecida correctamente.")
            self._resubscribe()
            return True
        except stomp.exception.ConnectFailedException as e:
            logger.error(f"Falló la conexión STOMP: {e}")
            self._is_connected = False
            return False
        except Exception as e:
            logger.error(f"Error inesperado durante conexión: {e}", exc_info=True)
            self._is_connected = False
            return False

    def connect(self):
        """Conecta a ActiveMQ con lógica de reintento."""
        inicio = time.time()
        while not self._connect_attempt():
            if time.time() - inicio >= config.MAX_RECONNECT_TIME:
                logger.critical(f"No se pudo conectar tras {config.MAX_RECONNECT_TIME} segundos. Saliendo.")
                self._should_run = False
                raise ConnectionError(f"No se pudo conectar a ActiveMQ tras {config.MAX_RECONNECT_TIME}s")
            logger.warning(f"Reintentando conexión en {config.RETRY_INTERVAL} segundos...")
            time.sleep(config.RETRY_INTERVAL)
        self._reconnect_scheduled = False

    def schedule_reconnect(self):
        """Programa reconexión si aún no ha sido agendada."""
        if not self._reconnect_scheduled:
            self._is_connected = False
            self._reconnect_scheduled = True
            logger.info("Reconexión programada.")

    def disconnect(self):
        """Desconexión controlada."""
        self._should_run = False
        if self.conn and self.is_connected():
            try:
                logger.info("Desconectando conexión STOMP...")
                self.conn.disconnect()
                logger.info("Conexión STOMP finalizada.")
            except Exception as e:
                logger.error(f"Error durante desconexión: {e}", exc_info=True)
            finally:
                self._is_connected = False
                self.conn = None

    def is_connected(self):
        """Verifica si el cliente sigue conectado."""
        return self._is_connected and self.conn and self.conn.is_connected()

    def send(self, destination, body, headers=None, content_type='application/json'):
        """Envía un mensaje."""
        if not self.is_connected():
            logger.error(f"No se puede enviar mensaje a {destination}. No hay conexión.")
            return False

        try:
            full_headers = {'content-type': content_type}
            if headers:
                full_headers.update(headers)

            body_str = json.dumps(body) if not isinstance(body, str) else body
            self.conn.send(destination=destination, body=body_str, headers=full_headers)
            logger.debug(f"Mensaje enviado a {destination}: {body_str[:100]}...")
            return True
        except Exception as e:
            logger.error(f"Fallo al enviar mensaje a {destination}: {e}", exc_info=True)
            return False

    def subscribe(self, destination, id, ack='auto'):
        """Se suscribe a un destino y lo guarda para futuras reconexiones."""
        if destination in self._subscriptions:
            logger.warning(f"Ya suscrito a {destination} con ID {self._subscriptions[destination]['id']}. Se sobrescribirá.")
        self._subscriptions[destination] = {'id': id, 'ack': ack}

        if self.is_connected():
            try:
                self.conn.subscribe(destination=destination, id=id, ack=ack)
                logger.info(f"Suscripción a {destination} con ID {id}")
            except Exception as e:
                logger.error(f"Error al suscribirse a {destination}: {e}", exc_info=True)
                self.schedule_reconnect()

    def _resubscribe(self):
        """Re-suscribe a todos los destinos guardados tras reconectar."""
        if not self.is_connected():
            logger.warning("No se puede re-suscribir, sin conexión.")
            return
        logger.info("Re-suscribiendo a destinos...")
        for dest, sub_info in self._subscriptions.items():
            try:
                self.conn.subscribe(destination=dest, id=sub_info['id'], ack=sub_info['ack'])
                logger.info(f"Re-suscripción exitosa a {dest} con ID {sub_info['id']}")
            except Exception as e:
                logger.error(f"Fallo al re-suscribirse a {dest}: {e}", exc_info=True)
                self.schedule_reconnect()
                break

    def run_forever(self):
        """Mantiene la conexión activa con reconexión automática."""
        self._should_run = True
        logger.info("Iniciando bucle de ejecución del cliente STOMP...")
        while self._should_run:
            try:
                if not self.is_connected():
                    logger.warning("Conexión no activa. Intentando reconexión.")
                    if self._reconnect_scheduled or self.conn is None:
                        self.connect()
                    else:
                        time.sleep(config.RETRY_INTERVAL / 2)
                        if not self.is_connected():
                            self.schedule_reconnect()
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Interrupción por teclado. Finalizando ejecución.")
                self._should_run = False
            except ConnectionError as ce:
                logger.critical(f"Conexión perdida de forma permanente: {ce}")
                self._should_run = False
            except Exception as e:
                logger.error(f"Error inesperado en el bucle de ejecución: {e}", exc_info=True)
                self.schedule_reconnect()
                time.sleep(config.RETRY_INTERVAL)

        logger.info("Bucle de ejecución del cliente STOMP finalizado.")
        self.disconnect()
