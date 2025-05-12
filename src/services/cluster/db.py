#src/services/cluster/db.py

from ...database import persist as agentes_db

SERVICE_NAME = "CLUSTER"

def init_db():
    """Initialize the database for the CLUSTER service."""
    agentes_db.init_service_table(SERVICE_NAME)

def is_project_processed(project_id):
    """Check if a project ID has already been processed."""
    return agentes_db.is_project_processed(SERVICE_NAME, project_id)

def mark_project_as_processed(project_id, status):
    """Mark a project ID as processed."""
    agentes_db.mark_project_as_processed(SERVICE_NAME, project_id, status)