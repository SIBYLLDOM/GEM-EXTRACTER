# config.py
"""
Global configuration for Dynamic Tender Extractor system.
Adjust only if needed. 
Default setup supports WAMP (MySQL root NO password) + local Redis.
"""

# -----------------------
# MySQL (WAMP phpMyAdmin)
# -----------------------
MYSQL = {
    "host": "127.0.0.1",       # Use 127.0.0.1 (NOT "localhost") for MySQL socket issues
    "port": 3306,
    "user": "root",
    "password": "",            # WAMP default = BLANK PASSWORD
    "db": "gem_extractor",     # DB you created using init_db.sql
    "charset": "utf8mb4"
}

# -----------------------
# Redis (Queue system)
# -----------------------
REDIS = {
    "host": "127.0.0.1",       # Local Redis server
    "port": 6379,
    "db": 0                    # Default DB index
}

# -----------------------
# Local directories
# -----------------------
# These folders must exist OR will be auto-created by scripts.
PDF_FOLDER = "PDF"            # where consumers download PDFs
OUTPUT_FOLDER = "OUTPUT"      # per-PDF parsed JSON objects
FULLDATA_FOLDER = "FULLDATA"  # merged JSON dictionary (data.json)

# -----------------------
# Worker + pipeline settings
# -----------------------
WORKER_ID = "worker-local"    # worker identity (can be overwritten per process)
DOWNLOAD_TIMEOUT = 60         # seconds for PDF download timeout
RETRY_LIMIT = 3               # attempts before marking as failed
LOCK_TIMEOUT = 180            # seconds before unlocking a stuck job (optional)

# For future expansion:
ENABLE_OCR = True             # allow parser to use OCR for low-text PDFs
OCR_THRESHOLD = 40            # min text chars before triggering OCR
