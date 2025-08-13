from piccolo.conf.apps import AppRegistry
from piccolo.engine.postgres import PostgresEngine
import os
from dotenv import load_dotenv

load_dotenv()
DB_2 = PostgresEngine(
    config={
        "database": os.getenv("POSTGRES_DB_2"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }
)
DB = PostgresEngine(
    config={
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    },
    extra_nodes={"DB_2": DB_2},
)


# DB.extra_nodes[DB_2] = DB_2
APP_REGISTRY = AppRegistry(apps=["new_agri_bot_backend.piccolo_app"])
