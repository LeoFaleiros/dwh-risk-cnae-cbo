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

# Two CKAN endpoints cover the full CAT history:
#   - "antigo": jul/2018 → mai/2023
#   - "pda":    jun/2023 → present (updated monthly)
_CKAN_URLS = [
    (
        "https://dadosabertos.inss.gov.br/api/3/action/package_show"
        "?id=inss-comunicacao-de-acidente-de-trabalho-cat"
    ),
    (
        "https://dadosabertos.inss.gov.br/api/3/action/package_show"
        "?id=comunicacoes-de-acidente-de-trabalho-cat-plano-de-dados-abertos-jun-2023-a-jun-2025"
    ),
]

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

# Output columns after normalization (common schema across all periods).
_OUTPUT_COLS = [
    "agente_causador",
    "data_competencia",
    "data_acidente",
    "cbo",
    "cid_10",
    "cnae_empregador_cod",
    "cnae_empregador_desc",
    "emitente",
    "especie_beneficio",
    "filiacao",
    "indica_obito",
    "munic_empregador",
    "natureza_lesao",
    "origem_cadastro",
    "parte_corpo",
    "sexo",
    "tipo_acidente",
    "uf_acidente",
    "uf_empregador",
    "data_despacho",
    "data_nascimento",
    "data_emissao",
    "cnpj_empregador",
    "_source_file",
]


def _list_all_resources() -> list[dict]:
    """Fetch resource lists from both CKAN endpoints, deduplicating by URL."""
    seen_urls: set[str] = set()
    all_resources: list[dict] = []
    for ckan_url in _CKAN_URLS:
        try:
            resp = requests.get(ckan_url, timeout=30)
            resp.raise_for_status()
            for r in resp.json()["result"]["resources"]:
                url = r.get("url", "")
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_resources.append(r)
        except Exception as exc:
            log.warning("Failed to fetch CKAN package %s: %s", ckan_url, exc)
    return all_resources


def _year_from_url(url: str) -> int | None:
    # Structured filenames: D.SDA.PDA.005.CAT.YYYYMM.ZIP
    m = re.search(r"\.CAT\.(\d{4})\d{2}\.", url, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Legacy filenames: cat2018-comp01-02-03-2019.csv, cat-jul-ago-set-2018.csv
    # The competência year is the LAST 4-digit number in the filename.
    # "cat2018-comp01-02-03-2019.csv" → 2019 (not 2018, which is the plan year)
    filename = url.split("/")[-1]
    matches = re.findall(r"(\d{4})", filename)
    if matches:
        return int(matches[-1])
    # Special case: "cat-copmp-04-05-06.zip" has no year — it's abr-jun 2021
    if "copmp" in filename.lower() or "04-05-06" in filename:
        return 2021
    return None


def _month_from_url(url: str) -> int | None:
    m = re.search(r"\.CAT\.(\d{4})(\d{2})\.", url, re.IGNORECASE)
    if m:
        return int(m.group(2))
    return None


def _resources_for_years(ano_ini: int, ano_fim: int) -> list[dict]:
    resources = _list_all_resources()
    result = []
    for r in resources:
        fmt = r.get("format", "").upper()
        if fmt not in ("CSV", "ZIP"):
            continue
        url = r.get("url", "")
        year = _year_from_url(url)
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


def _detect_schema(df: pd.DataFrame) -> str:
    """Detect which of the 3 CAT schemas this file uses.

    Schema A (2018-2020): CBO is 6-digit code, CBO.1 has description,
                          'Indica acidente' (not 'Indica Óbito Acidente'),
                          'Data Acidente' is YYYY/MM, 'Data Acidente.1' is DD/MM/YYYY.

    Schema B (2021-mai/2023): CBO is code+desc (45ch), no CBO.1,
                              'Indica Óbito Acidente',
                              'Data Acidente' is YYYY/MM, 'Data Acidente.2' is DD/MM/YYYY.

    Schema C (jun/2023+): CBO is 6-digit code, CBO.1 has description,
                          'Indica Óbito Acidente',
                          'Data Acidente' is DD/MM/YYYY directly.
    """
    cols = set(df.columns)
    has_cbo1 = "CBO.1" in cols
    has_data_acidente_2 = "Data Acidente.2" in cols

    if has_cbo1 and not has_data_acidente_2:
        # Could be A or C — distinguish by 'Indica acidente' vs 'Indica Óbito Acidente'
        if "Indica acidente" in cols:
            return "A"
        return "C"
    if not has_cbo1 and has_data_acidente_2:
        return "B"
    if has_cbo1 and has_data_acidente_2:
        return "B"  # fallback
    return "C"  # fallback


def _normalize(df: pd.DataFrame, source_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize any CAT schema to the common output format."""
    schema = _detect_schema(df)
    log.info("Detected schema %s for %s (%d cols)", schema, source_file, len(df.columns))

    # Strip whitespace from all string columns early
    str_cols = df.select_dtypes("object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    out = pd.DataFrame()

    # --- CNAE (same across all schemas) ---
    out["cnae_empregador_cod"] = df.get("CNAE2.0 Empregador")
    out["cnae_empregador_desc"] = df.get("CNAE2.0 Empregador.1")

    # --- CBO: extract 6-digit code ---
    if schema in ("A", "C"):
        out["cbo"] = df["CBO"]
    else:
        # Schema B: CBO is "515105-Agente Comunitário..." (45ch)
        # Split on dash to get code, then take first 6 digits
        out["cbo"] = df["CBO"].str.split("-", n=1).str[0].str.strip()

    # --- CID-10: extract code only ---
    if schema in ("A", "C"):
        out["cid_10"] = df["CID-10"].str.strip()
    else:
        # Schema B: CID-10 is "B34.2 Infecc p/Coronavirus Ne" (45ch)
        out["cid_10"] = df["CID-10"].str.split(" ", n=1).str[0].str.strip()

    # --- Indica Óbito ---
    if schema == "A":
        out["indica_obito"] = df.get("Indica acidente")
    else:
        out["indica_obito"] = df.get("Indica \xd3bito Acidente",
                                     df.get("Indica Óbito Acidente"))

    # --- Dates ---
    if schema == "A":
        # Data Acidente = "YYYY/MM", Data Acidente.1 = "DD/MM/YYYY"
        out["data_competencia"] = df.get("Data Acidente")
        out["data_acidente"] = df.get("Data Acidente.1")
    elif schema == "B":
        # Data Acidente = "YYYY/MM", Data Acidente.2 = "DD/MM/YYYY"
        out["data_competencia"] = df.get("Data Acidente")
        out["data_acidente"] = df.get("Data Acidente.2")
    else:
        # Schema C: Data Acidente = "DD/MM/YYYY" directly
        out["data_acidente"] = df.get("Data Acidente")
        # Derive competência from date (MM/YYYY → YYYY/MM)
        raw_date = df.get("Data Acidente", pd.Series(dtype=str))
        parts = raw_date.str.split("/")
        out["data_competencia"] = parts.apply(
            lambda p: f"{p[2]}/{p[1]}" if isinstance(p, list) and len(p) == 3 else None
        )

    # --- Simple renames (same name across all schemas, just encoding varies) ---
    col_map_simple = {
        "Agente  Causador  Acidente": "agente_causador",
        "Emitente CAT": "emitente",
        "Munic Empr": "munic_empregador",
        "Origem de Cadastramento CAT": "origem_cadastro",
        "Parte Corpo Atingida": "parte_corpo",
        "Sexo": "sexo",
        "Tipo do Acidente": "tipo_acidente",
        "UF  Munic.  Acidente": "uf_acidente",
        "UF Munic. Empregador": "uf_empregador",
        "Data Nascimento": "data_nascimento",
        "CNPJ/CEI Empregador": "cnpj_empregador",
    }
    for src, dst in col_map_simple.items():
        out[dst] = df.get(src)

    # Columns with encoding-dependent names — try both forms
    for candidates, dst in [
        (["Espécie do benefício", "Esp\xe9cie do benef\xedcio"], "especie_beneficio"),
        (["Filiação Segurado", "Filia\xe7\xe3o Segurado"], "filiacao"),
        (["Natureza da Lesão", "Natureza da Les\xe3o"], "natureza_lesao"),
        (["Data Despacho Benefício", "Data Despacho Benef\xedcio"], "data_despacho"),
        (["Data Emissão CAT", "Data Emiss\xe3o CAT"], "data_emissao"),
    ]:
        for cand in candidates:
            if cand in df.columns:
                out[dst] = df[cand]
                break
        if dst not in out.columns:
            out[dst] = None

    out["_source_file"] = source_file

    # Ensure all output columns exist
    for col in _OUTPUT_COLS:
        if col not in out.columns:
            out[col] = None

    out = out[_OUTPUT_COLS]

    # Quarantine: rows with no CNAE or no date
    required = ["cnae_empregador_cod", "data_acidente"]
    bad_mask = out[required].isnull().any(axis=1) | (out[required] == "").any(axis=1)
    quarantine = out[bad_mask].copy()
    good = out[~bad_mask].copy()

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
    ano_ini = config.cat_ano_inicio
    ano_fim = config.cat_ano_fim

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

            log.info("%s: %d rows (quarantine=%d)", filename, len(good), len(bad))
            frames.append(good)
        except Exception as exc:
            log.error("Failed to process %s: %s", filename, exc)

    if not frames:
        raise RuntimeError("No CAT data loaded — all files failed")

    return pd.concat(frames, ignore_index=True)
