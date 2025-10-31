from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "db": os.getenv("POSTGRES_DB"),
}

X_BEARER = os.getenv("X_BEARER")
