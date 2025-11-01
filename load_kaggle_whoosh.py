import pandas as pd
import psycopg2
import kagglehub
from config import POSTGRES

# ==================== DB Connection ====================
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES["db"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
        host=POSTGRES["host"],
        port=POSTGRES["port"]
    )

# ==================== Load Kaggle Whoosh Dataset ====================
def load_kaggle_whoosh():
    try:
        # Unduh dataset dari Kaggle (gevabriel/whoosh)
        path = kagglehub.dataset_download("gevabriel/whoosh")

        # Deteksi file CSV di folder dataset
        import os
        csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError("No CSV file found in Kaggle dataset folder.")
        
        csv_path = os.path.join(path, csv_files[0])
        print(f"Loading CSV file: {csv_path}")

        df = pd.read_csv(csv_path)

        # Tampilkan preview
        print("Dataset sample:")
        print(df.head())

        # Simpan ke database Postgres
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS kaggle_whoosh (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP,
            text TEXT,
            lang VARCHAR(10)
        );
        """)
        conn.commit()

        # Adaptasikan kolom agar sesuai dengan pipeline
        if "created_at" not in df.columns:
            # Jika kolom tidak ada, buat dummy timestamp
            import datetime
            df["created_at"] = datetime.datetime.now()

        if "text" not in df.columns:
            # Kolom wajib
            raise ValueError("Dataset must contain a 'text' column.")

        if "lang" not in df.columns:
            # Jika tidak ada, asumsikan bahasa Indonesia
            df["lang"] = "id"

        # Masukkan data ke database
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO kaggle_whoosh (created_at, text, lang)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (row["created_at"], row["text"], row["lang"]))

        conn.commit()
        cur.close()
        conn.close()

        print(f"Successfully loaded {len(df)} Kaggle Whoosh rows into database.")
        return df

    except Exception as e:
        print(f"Failed to load Kaggle Whoosh dataset: {e}")
        return pd.DataFrame()
