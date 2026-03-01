import glob

import pandas as pd


def fetch_dim_grau_risco(path: str = "input/AnexoI_CNAE_GR_NR04_2023_COMPLETO.xlsx") -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)

    # GR is at "Subclasse" level in the INSS file (e.g. "01.11-3" -> class code "01113")
    df = (
        df[df["Nível"] == "Subclasse"][["Código", "GR"]]
        .dropna(subset=["GR"])
        .rename(columns={"Código": "cnae_classe_raw", "GR": "grau_risco"})
        .assign(
            cnae_classe=lambda d: (
                d["cnae_classe_raw"].str.strip()
                .str.replace(r"[^\d]", "", regex=True)
                .str[:5]
            ),
            grau_risco=lambda d: d["grau_risco"].str.strip().astype(int),
        )
        [["cnae_classe", "grau_risco"]]
        .drop_duplicates(subset=["cnae_classe"])
        .reset_index(drop=True)
    )

    return df
