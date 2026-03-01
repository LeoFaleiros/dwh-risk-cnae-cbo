import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://dwh:dwh@localhost:5435/dwh_risk")
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    ingest_uf: str = os.getenv("INGEST_UF", "SP")
    ingest_ano_inicio: int = int(os.getenv("INGEST_ANO_INICIO", "2020"))
    ingest_ano_fim: int = int(os.getenv("INGEST_ANO_FIM", "2023"))


config = Config()
