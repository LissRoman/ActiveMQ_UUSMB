# src/database/persist.py

import sqlite3
import os
import re
from datetime import datetime

# Ruta absoluta hacia src/database/persist.db
DB_DIR = os.path.abspath(os.path.dirname(__file__))  
DB_PATH = os.path.join(DB_DIR, 'persist.db')

# Crear el directorio si no existe
os.makedirs(DB_DIR, exist_ok=True)

def get_connection():
    return sqlite3.connect(DB_PATH)
    print(f"📁 [DB] Usando base de datos en: {DB_PATH}")


def is_valid_table_name(name):
    return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name) is not None

def init_service_table(service_name):
    """Initialize the database and create the necessary table."""
    if not is_valid_table_name(service_name):
        raise ValueError(f"Nombre de tabla inválido: {service_name}")
    
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{service_name}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

def is_project_processed(service_name, project_id):
    """Check if a project ID has already been processed."""
    if not is_valid_table_name(service_name):
        raise ValueError(f"Nombre de tabla inválido: {service_name}")
    
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f'SELECT 1 FROM "{service_name}" WHERE project_id = ?', (project_id,))
        return cur.fetchone() is not None
    
def mark_project_as_processed(service_name, project_id, status):
    """Mark a project ID as processed."""
    if not is_valid_table_name(service_name):
        raise ValueError(f"Nombre de tabla inválido: {service_name}")
    
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f'''
            INSERT OR IGNORE INTO "{service_name}" (project_id, status)
            VALUES (?, ?)
        ''', (project_id, status))
        conn.commit()

        