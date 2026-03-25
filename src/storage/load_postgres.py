import pandas as pd
import psycopg2
import os


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5433"),
        dbname=os.getenv("PGDATABASE", "immobilier_db"),
        user=os.getenv("PGUSER", "airflow"),
        password=os.getenv("PGPASSWORD", "airflow")
    )


def create_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS posts (
        user_id INT,
        post_id INT PRIMARY KEY,
        title TEXT,
        body TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()


def load_csv_to_postgres(conn):
    df = pd.read_csv("data/processed/posts_clean.csv")

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO posts (user_id, post_id, title, body)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (post_id) DO NOTHING;
                """,
                (row["user_id"], row["post_id"], row["title"], row["body"])
            )
        conn.commit()


def main():
    conn = get_db_connection()
    create_table(conn)
    load_csv_to_postgres(conn)
    conn.close()
    print("Données chargées dans PostgreSQL")


if __name__ == "__main__":
    main()