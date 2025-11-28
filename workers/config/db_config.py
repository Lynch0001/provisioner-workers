# db_config.py
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://postgres:password@localhost:5432/provisioner",
    echo=False,
    future=True,
)
