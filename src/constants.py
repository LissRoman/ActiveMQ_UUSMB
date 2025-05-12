# src/constants.py

# Message Types
MESSAGE_TYPES = {
    "START": "START",
    "PROCESS": "PROCESS",
    "RESPONSE": "RESPONSE",
    "STATUS": "STATUS",
    "ERROR": "ERROR",
}

# Service Names
SERVICES = {
    "SISBI": "SISBI",
    "LISS": "LISS",
    "WATSON": "WATSON",
    "CLUSTER": "CLUSTER",
}

# Process Status
PROCESS_STATUS = {
    "STARTED": "STARTED",
    "IN_PROGRESS": "IN_PROGRESS",
    "COMPLETED": "COMPLETED",
    "FAILED": "FAILED",
    "TIMEOUT": "TIMEOUT",
    "DUPLICATE_COMPLETED": "DUPLICATE_COMPLETED",  # Completado porque ya se había procesado antes
}

# Simulation Defaults
DEFAULT_PROJECT_ID = "default-proj-123"
DEFAULT_MESSAGE = "Start default simulation"
