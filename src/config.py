# src/config.py
import os

# Detalles de conexión a ActiveMQ
HOST = os.getenv("ACTIVEMQ_HOST", "localhost")
PORT = int(os.getenv("ACTIVEMQ_PORT", 61613))
USERNAME = os.getenv("ACTIVEMQ_USER", "admin")
PASSWORD = os.getenv("ACTIVEMQ_PASS", "admin")

# Nombres de cola
SISBI_QUEUE = "/queue/sisbi"
LISS_QUEUE = "/queue/liss"
WATSON_QUEUE = "/queue/watson"
CLUSTER_QUEUE = "/queue/cluster" 

# Service URLs

# Connection Retry Logic
RETRY_INTERVAL = 10  # segundos
MAX_RECONNECT_TIME = 60 # segundos

# Logging
LOG_DIRECTORY = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
if not os.path.exists(LOG_DIRECTORY):
    os.makedirs(LOG_DIRECTORY)

SISBI_LOG_FILE = os.path.join(LOG_DIRECTORY, 'sisbi.log')
LISS_LOG_FILE = os.path.join(LOG_DIRECTORY, 'liss.log')
WATSON_LOG_FILE = os.path.join(LOG_DIRECTORY, 'watson.log')
CLUSTER_LOG_FILE = os.path.join(LOG_DIRECTORY, 'cluster.log')

# FastAPI (for SISBI)
API_HOST = "0.0.0.0"
API_PORT = 8000