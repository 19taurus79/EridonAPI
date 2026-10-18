"""
Pytest configuration for EridonAPI tests.
Ensures tests run strictly against test PostgreSQL environment on port 5437
without polluting environment variables for existing test suites.
"""

import os
from pathlib import Path
from dotenv import dotenv_values

test_env_path = Path("D:/Projects/EridonAPI/.env.test").resolve()
if test_env_path.exists():
    vals = dotenv_values(test_env_path)
    # Set only DB connection params for test PostgreSQL on port 5437
    for k in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]:
        if k in vals and vals[k]:
            os.environ[k] = vals[k]

os.environ["POSTGRES_HOST"] = "127.0.0.1"
os.environ["POSTGRES_PORT"] = "5437"

# Clean any leaking gateway or webhook variables so unit tests are isolated
os.environ.pop("MCP_GATEWAY_URL", None)
os.environ.pop("MCP_INTERNAL_SECRET", None)
os.environ.pop("MANAGE_TELEGRAM_WEBHOOK", None)

# Update Piccolo DB config
try:
    import piccolo_conf
    if hasattr(piccolo_conf, "DB") and hasattr(piccolo_conf.DB, "config"):
        piccolo_conf.DB.config["port"] = 5437
        piccolo_conf.DB.config["host"] = "127.0.0.1"
        if "POSTGRES_DB" in os.environ:
            piccolo_conf.DB.config["database"] = os.environ["POSTGRES_DB"]
        if "POSTGRES_USER" in os.environ:
            piccolo_conf.DB.config["user"] = os.environ["POSTGRES_USER"]
        if "POSTGRES_PASSWORD" in os.environ:
            piccolo_conf.DB.config["password"] = os.environ["POSTGRES_PASSWORD"]
except Exception:
    pass
