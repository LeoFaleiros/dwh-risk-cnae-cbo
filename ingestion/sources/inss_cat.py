import io
import logging
import re
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from ingestion.config import config

log = logging.getLogger(__name__)

# CKAN package endpoint for the CAT dataset
_CKAN_URL = (
    "https://dadosabertos.inss.gov.br/api/3/action/package_show"
    "?id=inss-comunicacao-de-acidente-de-trabalho-cat"
)

# Map UF sigla to the full name used in the 'UF Munic. Empregador' column
_UF_NAMES = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal",
    "ES": "Espírito Santo", "GO": "Goiás", "MA": "Maranhão",
    "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco",
    "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima",
    "SC": "Santa Catarina", "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins",
}

# Explicit mapping from raw column names to normalized names
_COL_MAP = {
    "Agente  Causador  Acidente": "agente_causador",
    "Data Acidente":              "data_competencia",
    "Data Acidente.1":            "_drop",
    "Data Acidente.2":            "data_acidente",
    "CBO":                        "cbo",
    "CID-10":                     "cid_10",
    "CNAE2.0 Empregador":         "cnae_empregador_cod",
    "CNAE2.0 Empregador.1":       "cnae_empregador_desc",
    "Emitente CAT":               "emitente",
    "Espécie do benefício":       "especie_beneficio",
    "Filiação Segurado":          "filiacao",
    "Indica Óbito Acidente":      "indica_obito",
    "Munic Empr":                 "munic_empregador",
    "Natureza da Lesão":          "natureza_lesao",
    "Origem de Cadastramento CAT": "origem_cadastro",
    "Parte Corpo Atingida":       "parte_corpo",
    "Sexo":                       "sexo",
    "Tipo do Acidente":           "tipo_acidente",
    "UF  Munic.  Acidente":       "uf_acidente",
    "UF Munic. Empregador":       "uf_empregador",
    "Data Despacho Benefício":    "data_despacho",
    "Data Nascimento":            "data_nascimento",
    "Data Emissão CAT":           "data_emissao",
    "CNPJ/CEI Empregador":        "cnpj_empregador",
}


def _list_resources() -> list[dict]:
    resp = requests.get(_CKAN_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()["result"]["resources"]


def _year_from_url(url: str) -> int | None:
    # Matches both CAT.YYYYMM.ZIP and cat-comp01-02-03-2022.zip patterns
    m = re.search(r"\.CAT\.(\d{4})\d{2}\.", url, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"[-_](\d{4})[-_\.]", url)
    if m:
        return int(m.group(1))
    return None


def _resources_for_years(ano_ini: int, ano_fim: int) -> list[dict]:
    resources = _list_resources()
    result = []
    for r in resources:
        if r.get("format", "").upper() not in ("CSV", "ZIP"):
            continue
        year = _year_from_url(r["url"])
        if year is None:
            continue
        if ano_ini <= year <= ano_fim:
            result.append(r)
    return result


def _read_csv_from_response(content: bytes, filename: str) -> pd.DataFrame:
    if filename.upper().endswith(".ZIP"):
        z = zipfile.ZipFile(io.BytesIO(content))
        csv_names = [n for n in z.namelist() if n.upper().endswith(".CSV")]
        if not csv_names:
            raise ValueError(f"No CSV found inside {filename}")
        with z.open(csv_names[0]) as f:
            return pd.read_csv(f, encoding="latin-1", sep=";", dtype=str)
    return pd.read_csv(io.BytesIO(content), encoding="latin-1", sep=";", dtype=str)


def _normalize(df: pd.DataFrame, source_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.rename(columns=_COL_MAP)
    drop_cols = [c for c in df.columns if c == "_drop"]
    df = df.drop(columns=drop_cols, errors="ignore")

    # Strip whitespace from all string columns
    str_cols = df.select_dtypes("object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    df["_source_file"] = source_file

    # Quarantine: rows with no municipality or no date
    required = ["munic_empregador", "data_acidente"]
    missing = required if not all(c in df.columns for c in required) else required
    bad_mask = df[missing].isnull().any(axis=1) | (df[missing] == "").any(axis=1)
    quarantine = df[bad_mask].copy()
    good = df[~bad_mask].copy()

    return good, quarantine


def _write_quarantine(rows: pd.DataFrame, source_file: str) -> None:
    if rows.empty:
        return
    out_dir = Path("data/quarantine/cat")
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(source_file).stem
    out_path = out_dir / f"{stem}_{date.today()}.csv"
    rows.to_csv(out_path, index=False)
    log.warning("Quarantined %d rows from %s -> %s", len(rows), source_file, out_path)


def fetch_cat_microdados() -> pd.DataFrame:
    uf = config.ingest_uf
    ano_ini = config.ingest_ano_inicio
    ano_fim = config.ingest_ano_fim
    uf_name = _UF_NAMES.get(uf.upper())
    if not uf_name:
        raise ValueError(f"Unknown UF sigla: {uf}")

    resources = _resources_for_years(ano_ini, ano_fim)
    if not resources:
        raise ValueError(f"No CAT resources found for years {ano_ini}-{ano_fim}")

    log.info("Found %d CAT files for %d-%d", len(resources), ano_ini, ano_fim)

    frames = []
    for r in resources:
        url = r["url"]
        filename = url.split("/")[-1]
        log.info("Downloading %s...", filename)
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            raw_df = _read_csv_from_response(resp.content, filename)
            good, bad = _normalize(raw_df, filename)
            _write_quarantine(bad, filename)

            # Filter to target UF
            if "uf_empregador" in good.columns:
                good = good[good["uf_empregador"] == uf_name]

            log.info("%s: %d rows (UF=%s, quarantine=%d)", filename, len(good), uf, len(bad))
            frames.append(good)
        except Exception as exc:
            log.error("Failed to process %s: %s", filename, exc)

    if not frames:
        raise RuntimeError("No CAT data loaded — all files failed")

    return pd.concat(frames, ignore_index=True)
