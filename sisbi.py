# sisbi.py
import stomp
import time
import config  # Importamos la configuración
from fastapi import FastAPI
from pydantic import BaseModel
import threading
import os

app = FastAPI()

# Modelo para recibir el mensaje personalizado
class MessagePayload(BaseModel):
    message: str

RETRY_INTERVAL = 10          # Intervalo de reintento en segundos
MAX_RECONNECT_TIME = 60     # Tiempo máximo de reintentos (5 minutos)

class SISBIListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, frame):
        print(f'SISBI: Recibido un error: {frame.body}')

    def on_message(self, frame):
        print(f'SISBI: Recibido un mensaje: {frame.body}')
       

def reconnect():
    start_time = time.time()
    while True:
        try:
            conn = stomp.Connection([(config.HOST, config.PORT)])
            conn.set_listener('', SISBIListener(conn))
            conn.connect(config.USERNAME, config.PASSWORD, wait=True)
            print("SISBI: Conexión establecida")
            return conn
        except Exception as e:
            print(f"SISBI: Error de conexión. {e}")
            elapsed = time.time() - start_time
            if elapsed >= MAX_RECONNECT_TIME:
                print(f"SISBI: No se pudo conectar en {MAX_RECONNECT_TIME} segundos... CERRANDO...")
                os._exit(0)
            
            print("SISBI:Intentando reconectar en 10 segundos...")
            time.sleep(RETRY_INTERVAL)

def simulation(custom_message: str):
    conn= reconnect()
    conn.subscribe(destination=config.SISBI_QUEUE, id=1, ack='auto')
    
    # Enviar el mensaje personalizado
    print(f"SISBI: Enviando mensaje: {custom_message}")
    conn.send(body=custom_message, destination=config.LISS_QUEUE)
    
    try:
        # Mantener la conexión activa (simulación de la app web)
        while True:
            time.sleep(1)
            if not conn.is_connected():
                print("SISBI: Conexión perdida. Intentando reconectar...")
                conn = reconnect()
                conn.subscribe(destination=config.SISBI_QUEUE, id=1, ack='auto')

    except KeyboardInterrupt:
        print("SISBI: Desconectando...")
    finally:
        if conn.is_connected():
            conn.disconnect()
        print("SISBI: Desconectado.")

@app.post("/simulacion")
def simulacion_endpoint(payload: MessagePayload):
    # Inicia la simulación en un hilo de fondo para no bloquear el endpoint
    thread = threading.Thread(target=simulation, args=(payload.message,), daemon=True)
    thread.start()
    return {"message": "Simulación iniciada", "custom_message": payload.message}

if __name__ == '__main__':
    print("Verificando conexión inicial con el servidor...")
    conn_test = reconnect()
    if conn_test.is_connected():
        print("Conexión verificada. Iniciando FastAPI...")
        conn_test.disconnect()
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)


