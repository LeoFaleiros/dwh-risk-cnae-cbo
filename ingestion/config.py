import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def _int(env: str, default: str) -> int:
    return int(os.getenv(env, default))


@dataclass
class Config:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://dwh:dwh@localhost:5435/dwh_risk")
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    ingest_uf: str = os.getenv("INGEST_UF", "SP")

    # Per-source year ranges (fall back to global INGEST_ANO_* if source-specific not set)
    ingest_ano_inicio: int = _int("INGEST_ANO_INICIO", "2020")
    ingest_ano_fim: int = _int("INGEST_ANO_FIM", "2023")

    @property
    def rais_ano_inicio(self) -> int:
        return _int("INGEST_RAIS_ANO_INICIO", "2018")

    @property
    def rais_ano_fim(self) -> int:
        return _int("INGEST_RAIS_ANO_FIM", "2024")

    @property
    def sim_ano_inicio(self) -> int:
        return _int("INGEST_SIM_ANO_INICIO", str(self.ingest_ano_inicio))

    @property
    def sim_ano_fim(self) -> int:
        return _int("INGEST_SIM_ANO_FIM", str(self.ingest_ano_fim))

    @property
    def cat_ano_inicio(self) -> int:
        return _int("INGEST_CAT_ANO_INICIO", "2018")

    @property
    def cat_ano_fim(self) -> int:
        return _int("INGEST_CAT_ANO_FIM", "2025")


config = Config()
