# watson.py
import stomp
import time
import config

import os
RETRY_INTERVAL = 10          # Intervalo de reintento en segundos
MAX_RECONNECT_TIME = 60     # Tiempo máximo de reintentos (5 minutos)

class WatsonListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, frame):
        print(f'WATSON: Recibido un error: {frame.body}')

    def on_message(self, frame):
        message = frame.body
        print(f'WATSON: Recibido: {message}')

        if message.startswith('PROCESAR:'):
            identificador = message.split(':')[1]
            self.simular_proceso(identificador)

    def simular_proceso(self, identificador):
        print(f'WATSON: Iniciando proceso de clonación (simulado) para ID: {identificador}')
        for i in range(1, 11):
            print(f'WATSON: Clonando... {i}/10')
            time.sleep(0.5)  # Simula un retardo
        print(f'WATSON: Proceso de clonación completado para ID: {identificador}')
        self.conn.send(body=f'WATSON_COMPLETADO:{identificador}', destination=config.LISS_QUEUE)

def reconnect():
    start_time = time.time()
    while True:
        try:
            conn = stomp.Connection([(config.HOST, config.PORT)])
            conn.set_listener('', WatsonListener(conn))
            conn.connect(config.USERNAME, config.PASSWORD, wait=True) 
            print("WATSON: Conexión establecida")
            return conn
        except Exception as e:
            print(f"WATSON: Error de conexión {e}")
            elapsed = time.time() - start_time
            if elapsed >= MAX_RECONNECT_TIME:
                print(f"WATSON: No se pudo conectar en {MAX_RECONNECT_TIME} segundos... CERRANDO...")
                os._exit(0)
            print("WATSON: Intentando reconectar en 10 segundos...")
            time.sleep(RETRY_INTERVAL)

def main():
    conn = reconnect() 
    conn.subscribe(destination=config.WATSON_QUEUE, id=1, ack='auto')
    print('WATSON: Esperando mensajes...')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("WATSON: Desconectando...")
    finally:
      if conn.is_connected():
        conn.disconnect()
      print("WATSON: Desconectado")


if __name__ == '__main__':
    main()