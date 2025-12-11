from dotenv import load_dotenv
import os

load_dotenv(".env")

POSTGRE_DB_HOST = os.getenv("POSTGRE_DB_HOST")
POSTGRE_DB_PORT = os.getenv("POSTGRE_DB_PORT")
POSTGRE_DB_USER = os.getenv("POSTGRE_DB_USER")
POSTGRE_DB_PASS = os.getenv("POSTGRE_DB_PASS")
POSTGRE_DB_NAME = os.getenv("POSTGRE_DB_NAME")
POSTGRE_DB_CONN = (
    f"postgresql://{POSTGRE_DB_USER}:{POSTGRE_DB_PASS}@{POSTGRE_DB_HOST}:{POSTGRE_DB_PORT}/{POSTGRE_DB_NAME}"
)
