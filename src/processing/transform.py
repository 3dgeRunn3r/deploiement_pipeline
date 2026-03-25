import json
from pathlib import Path
import pandas as pd


def get_latest_raw_file():
    files = list(Path("data/raw").glob("*.json"))
    if not files:
        raise FileNotFoundError("Aucun fichier JSON trouvé dans data/raw")
    return max(files, key=lambda x: x.stat().st_mtime)


def load_raw_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_data(data):
    df = pd.DataFrame(data)

    df = df.rename(columns={
        "userId": "user_id",
        "id": "post_id"
    })

    df = df[["user_id", "post_id", "title", "body"]]

    return df


def save_processed_data(df):
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "posts_clean.csv"
    df.to_csv(output_file, index=False, encoding="utf-8")

    print(f"Données transformées sauvegardées : {output_file}")


def main():
    raw_file = get_latest_raw_file()
    print(f"Fichier brut utilisé : {raw_file}")

    data = load_raw_data(raw_file)
    df = transform_data(data)
    save_processed_data(df)


if __name__ == "__main__":
    main()