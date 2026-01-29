from pathlib import Path
import pandas as pd

from clean_data import clean_customers_df  # si tu lances depuis main/
# sinon (si tu lances en module): from main.clean_data import clean_customers_df


def find_project_root() -> Path:
    """
    Racine = dossier parent de /main (donc .../data ops)
    """
    return Path(__file__).resolve().parents[1]


def run_customers_pipeline() -> None:
    project = find_project_root()
    print("PROJECT =", project)

    raw_dir = project / "data" / "raw"
    clean_dir = project / "data" / "clean"

    print("RAW_DIR  =", raw_dir, "| exists:", raw_dir.exists())
    print("CLEAN_DIR=", clean_dir)

    clean_dir.mkdir(parents=True, exist_ok=True)

    files = [
        "customers_dirty.csv",
        "customers_dirty2.csv",
        "customers_dirty3.csv",
    ]

    for i, fname in enumerate(files, start=1):
        in_path = raw_dir / fname
        out_path = clean_dir / f"customers_clean_{i}.csv"

        print("\nIN =", in_path)
        if not in_path.exists():
            raise FileNotFoundError(f"Fichier introuvable : {in_path}")

        df = pd.read_csv(in_path)
        df_clean = clean_customers_df(df)
        df_clean.to_csv(out_path, index=False)

        print(f"OK {fname} -> {out_path.name} | {len(df)} -> {len(df_clean)} lignes")


if __name__ == "__main__":
    run_customers_pipeline()