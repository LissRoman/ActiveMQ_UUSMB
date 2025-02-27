# cluster.py
import stomp
import time
import config

import os 
RETRY_INTERVAL = 10          # Intervalo de reintento en segundos
MAX_RECONNECT_TIME = 60      # Tiempo máximo de reintentos en segundos (1 minuto)

class ClusterListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, frame):
        print(f'CLUSTER: Recibido un error: {frame.body}')

    def on_message(self, frame):
        message = frame.body
        print(f'CLUSTER: Recibido: {message}')

        if message.startswith('PROCESAR:'):
            identificador = message.split(':')[1]
            self.simular_proceso(identificador)

    def simular_proceso(self, identificador):
        print(f'CLUSTER: Iniciando proceso (simulado) para ID: {identificador}')
        time.sleep(2)  # Simula un proceso que tarda 2 segundos
        print(f'CLUSTER: Proceso completado para ID: {identificador}')
        self.conn.send(body=f'CLUSTER_COMPLETADO:{identificador}', destination=config.LISS_QUEUE)

def reconnect():
    start_time = time.time()
    while True:
        try:
            conn = stomp.Connection([(config.HOST, config.PORT)])
            conn.set_listener('', ClusterListener(conn))
            conn.connect(config.USERNAME, config.PASSWORD, wait=True)
            print("CLUSTER: Conexión establecida")
            return conn
        except Exception as e:
            print(f"CLUSTER: Error de conexión {e}")
            elapsed = time.time() - start_time
            if elapsed >= MAX_RECONNECT_TIME:
                print(f"CLUSTER: No se pudo conectar en {MAX_RECONNECT_TIME} segundos... CERRANDO...")
                os._exit(0)
            print("CLUSTER: Intentando reconectar en 10 segundos...")
            time.sleep(RETRY_INTERVAL) 

def main():
    conn = reconnect()
    conn.subscribe(destination=config.CLUSTER_QUEUE, id=1, ack='auto')

    print('CLUSTER: Esperando mensajes...')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("CLUSTER: Desconectando...")
    finally:
      if conn.is_connected():
        conn.disconnect()
      print("CLUSTER: Desconectado.")

if __name__ == '__main__':
    main()