"""
Load official CID-10 codes and descriptions from local DATASUS CSV files.

Source files (input/CID/):
  CID-10-SUBCATEGORIAS.CSV  – 4-char codes (e.g. A000) + full descriptions
  CID-10-CATEGORIAS.CSV     – 3-char category codes (e.g. A00) + descriptions

Subcategory codes are stored without a decimal point in DATASUS files (A000).
stg_fact_cat.cid_10 is already in dotted format (A00.0), so we insert the dot
by splitting before the last character: A000 → A00.0.

Category-level codes (A00) are loaded as-is and serve as fallback for CAT
records that carry only the 3-char code.
"""

import pandas as pd

_SUBCAT_PATH = "input/CID/CID-10-SUBCATEGORIAS.CSV"
_CAT_PATH = "input/CID/CID-10-CATEGORIAS.CSV"
_ENCODING = "cp1252"  # DATASUS Windows Latin-1 encoding
_SEP = ";"


def _dot_subcat(code: str) -> str:
    """Convert undotted 4-char code to dotted: 'A000' → 'A00.0'."""
    code = code.strip()
    if len(code) >= 4:
        return code[:3] + "." + code[3:]
    return code


def fetch_dim_cid(
    subcat_path: str = _SUBCAT_PATH,
    cat_path: str = _CAT_PATH,
) -> pd.DataFrame:
    """
    Return DataFrame with columns:
      cid_codigo   – dotted code (A00.0) or category code (A00)
      cid_descricao – full Portuguese description
    """
    # --- subcategories (A00.0 style) ---
    subcat = pd.read_csv(subcat_path, sep=_SEP, encoding=_ENCODING, dtype=str)
    subcat = (
        subcat[["SUBCAT", "DESCRICAO"]]
        .dropna(subset=["SUBCAT", "DESCRICAO"])
        .replace("", pd.NA)
        .dropna(subset=["SUBCAT", "DESCRICAO"])
        .assign(
            cid_codigo=lambda d: d["SUBCAT"].str.strip().apply(_dot_subcat),
            cid_descricao=lambda d: d["DESCRICAO"].str.strip(),
        )
        [["cid_codigo", "cid_descricao"]]
    )

    # --- categories (A00 style – 3-char fallback) ---
    cat = pd.read_csv(cat_path, sep=_SEP, encoding=_ENCODING, dtype=str)
    cat = (
        cat[["CAT", "DESCRICAO"]]
        .dropna(subset=["CAT", "DESCRICAO"])
        .replace("", pd.NA)
        .dropna(subset=["CAT", "DESCRICAO"])
        .assign(
            cid_codigo=lambda d: d["CAT"].str.strip(),
            cid_descricao=lambda d: d["DESCRICAO"].str.strip(),
        )
        [["cid_codigo", "cid_descricao"]]
    )

    combined = (
        pd.concat([subcat, cat], ignore_index=True)
        .drop_duplicates(subset=["cid_codigo"])
        .reset_index(drop=True)
    )
    return combined
