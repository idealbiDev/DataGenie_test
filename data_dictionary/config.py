import os
from dotenv import load_dotenv

load_dotenv()  # Load from .env file

class Config:
    # Local SQL Server
    LOCAL_DB = {
        "driver": os.getenv("LOCAL_DB_DRIVER", "ODBC Driver 17 for SQL Server"),
        "server": os.getenv("LOCAL_DB_SERVER", "localhost\\SQLEXPRESS"),
        "database": os.getenv("LOCAL_DB_NAME", "YourDatabase"),
        "username": os.getenv("LOCAL_DB_USER", "sa"),
        "password": os.getenv("LOCAL_DB_PASSWORD", "")
    }

    # Redshift (AWS)
    REDSHIFT = {
        "host": os.getenv("REDSHIFT_HOST"),
        "port": os.getenv("REDSHIFT_PORT", "5439"),
        "database": os.getenv("REDSHIFT_DB"),
        "user": os.getenv("REDSHIFT_USER"),
        "password": os.getenv("REDSHIFT_PASSWORD")
    }

    # Cohere
    COHERE_API_KEY = os.getenv("COHERE_API_KEY")
    COHERE_MODEL = os.getenv("COHERE_MODEL", "command-r-plus")

    # Dictionary Storage
    STORAGE_DB = os.getenv("STORAGE_DB", "local")  # 'local' or 'redshift'