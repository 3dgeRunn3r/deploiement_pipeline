import boto3
from botocore.client import Config
from pathlib import Path


def upload_latest_file():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    bucket_name = "raw-data"

    files = list(Path("data/raw").glob("*.json"))
    if not files:
        raise Exception("Aucun fichier trouvé dans data/raw")

    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    print(f"Fichier trouvé : {latest_file}")

    s3.upload_file(str(latest_file), bucket_name, latest_file.name)

    print(f"Upload réussi → {bucket_name}/{latest_file.name}")


if __name__ == "__main__":
    upload_latest_file()