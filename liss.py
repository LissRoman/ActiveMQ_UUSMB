# liss.py
import stomp
import time
import config

import os
RETRY_INTERVAL = 10          # Intervalo de reintento en segundos
MAX_RECONNECT_TIME = 60      # Tiempo máximo de reintentos en segundos (1 minuto)

class LISSListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn
        self.waiting_for_watson = False
        self.identificador = None #Guarda el identificador temporalmente

    def on_error(self, frame):
        print(f'LISS: Recibido un error: {frame.body}')

    def on_message(self, frame):
        message = frame.body
        print(f'LISS: Recibido: {message}')

        if message.startswith('INICIO:'):
            self.identificador = message.split(':')[1]
            print(f'LISS: Enviando mensaje a WATSON para ID: {self.identificador}')
            self.conn.send(body=f'PROCESAR:{self.identificador}', destination=config.WATSON_QUEUE)
            self.waiting_for_watson = True

        elif message.startswith('WATSON_COMPLETADO:'):
            received_id = message.split(':')[1]
            if received_id == self.identificador: #Verifica que corresponda a este proceso
                print(f'LISS: WATSON completó el proceso para ID: {received_id}')
                print('LISS: Enviando STATUS a SISBI')
                self.conn.send(body=f'STATUS:COMPLETADO:{received_id}', destination=config.SISBI_QUEUE)
                print('LISS: Enviando mensaje a CLUSTER')
                self.conn.send(body=f'PROCESAR:{received_id}', destination=config.CLUSTER_QUEUE)
                self.waiting_for_watson = False # Resetea el estado
                self.identificador = None
            else:
                print(f"LISS: Recibido WATSON_COMPLETADO de un proceso no relacionado ({received_id}). Ignorando.")

        elif message.startswith("CLUSTER_COMPLETADO:"):
            print(f"LISS: Recibi confirmacion que CLUSTER completo el proceso")

def reconnect():
    start_time = time.time()
    while True:
        try:
            conn = stomp.Connection([(config.HOST, config.PORT)])
            conn.set_listener('', LISSListener(conn))
            conn.connect(config.USERNAME, config.PASSWORD, wait=True)
            print("LISS: Conexión esteblecida")
            return conn 
        except Exception as e:
            print(f"LISS: Error de conexión {e}")
            elapsed = time.time() - start_time
            if elapsed >= MAX_RECONNECT_TIME:
                print(f"LISS: No se pudo conectar en {MAX_RECONNECT_TIME} segundos... CERRANDO...")
                os._exit(0)
            print("LISS: Intentando reconectar en 10 segundos...")
            time.sleep(RETRY_INTERVAL)  

def main():
    conn = reconnect()
    conn.subscribe(destination=config.LISS_QUEUE, id=1, ack='auto')

    print('LISS: Esperando mensajes...')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("LISS: Desconectando...")
    finally:
      if conn.is_connected():
        conn.disconnect()
      print("LISS: Desconectado")

if __name__ == '__main__':
    main()