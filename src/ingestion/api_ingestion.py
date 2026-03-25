import os
import json
import requests
from pathlib import Path
from datetime import datetime


def run_ingestion():
    api_url = os.getenv("API_URL")

    if not api_url:
        raise ValueError("API_URL not found")

    print(f"Fetching data from {api_url}")

    response = requests.get(api_url, timeout=20)
    response.raise_for_status()

    data = response.json()

    # dossier data/raw
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    # nom du fichier avec date
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"data_{timestamp}.json"

    # sauvegarde
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Data saved to {file_path}")


if __name__ == "__main__":
    run_ingestion()