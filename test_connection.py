from sqlalchemy import create_engine
import pandas as pd

# Ganti user/password sesuai setting kamu
engine = create_engine("postgresql+psycopg2://loupidd:070809@localhost:5432/whoosh_analysis")

try:
    with engine.connect() as conn:
        df = pd.read_sql("SELECT version();", conn)
        print(df)
    print("Koneksi berhasil!")
except Exception as e:
    print("Gagal menyambungkan:", e)
