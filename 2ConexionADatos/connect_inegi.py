import os
import glob
import zipfile
import requests
import pandas as pd

RAW_DIR_INEGI = "./data/raw/inegi"
PROCESSED_DIR = "./data/processed"

INEGI_ZIP_URL = "https://www.inegi.org.mx/contenidos/programas/accidentes/datosabiertos/conjunto_de_datos_atus_anual_csv.zip"


def download_inegi_zip():
    """Descarga el ZIP de INEGI si no existe."""
    os.makedirs(RAW_DIR_INEGI, exist_ok=True)
    zip_path = os.path.join(RAW_DIR_INEGI, "inegi_atus.zip")

    if not os.path.exists(zip_path):
        print("⬇️ Descargando datos de INEGI...")
        r = requests.get(INEGI_ZIP_URL)
        with open(zip_path, "wb") as f:
            f.write(r.content)
        print("   ✓ Descarga completada")
    else:
        print("📦 ZIP ya existe, no se descarga nuevamente.")

    return zip_path


def extract_inegi_zip(zip_path):
    """Extrae todos los CSV del ZIP."""
    print("📂 Extrayendo CSV del ZIP...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(RAW_DIR_INEGI)
    print("   ✓ Archivos extraídos")


def tidy_inegi_data(year_range=(2018, 2024)):
    print("\n🧹 Limpiando datos INEGI...")

    pattern = os.path.join(RAW_DIR_INEGI+"/conjunto_de_datos/", "atus_anual_*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        print("⚠️ No hay CSV extraídos.")
        return None

    dfs = []
    ymin, ymax = year_range

    for file in files:
        filename = os.path.basename(file)
        try:
            year = int(filename.split("_")[-1].replace(".csv", ""))
        except:
            continue

        if year < ymin or year > ymax:
            continue

        print(f"   📄 Leyendo {filename}...")
        df = pd.read_csv(file, encoding="utf-8", low_memory=False)
        df["AÑO"] = year
        dfs.append(df)

    if not dfs:
        print("⚠️ No se cargaron datos.")
        return None

    df_all = pd.concat(dfs, ignore_index=True)

    # Limpieza ligera (CRISP-DM Etapa 2 = NO limpiamos demasiado todavía)
    df_all.columns = df_all.columns.str.strip().str.upper()
    df_clean = df_all.drop_duplicates()

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output = os.path.join(PROCESSED_DIR, "inegi_tidy.csv")
    df_clean.to_csv(output, index=False)

    print(f"   ✓ Datos consolidados en {output}")
    return df_clean


if __name__ == "__main__":
    zip_path = download_inegi_zip()
    # extract_inegi_zip(zip_path)
    tidy_inegi_data()
