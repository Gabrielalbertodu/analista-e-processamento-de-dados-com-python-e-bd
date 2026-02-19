import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def _build_engine():
    url = os.getenv("DATABASE_URL")
    if url:
        return create_engine(url)
    vendor = os.getenv("DB_VENDOR", "sqlite").lower()
    if vendor == "mysql":
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "3306")
        name = os.getenv("DB_NAME")
        if not all([user, password, name]):
            raise ValueError("Configure DB_USER, DB_PASSWORD e DB_NAME para usar MySQL")
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"
        return create_engine(url)
    path = os.getenv("DB_PATH", os.path.join(os.getcwd(), "health_data.db"))
    url = f"sqlite:///{path}"
    return create_engine(url)

engine = _build_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_engine():
    return engine
