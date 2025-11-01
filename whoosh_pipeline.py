import requests
import psycopg2
import pandas as pd
import hashlib
import time
import re
import os
import kagglehub
from config import POSTGRES, X_BEARER

KEYWORDS = ["kereta cepat whoosh", "whoosh", "kcic"]
LANGUAGES = ["id", "en"]
TWEETS_TARGET = 5000
DELAY = 2

# ==================== DB Connection ====================
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES["db"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
        host=POSTGRES["host"],
        port=POSTGRES["port"]
    )

# ==================== Twitter/X Request =================
def collect_x(query, next_token=None):
    headers = {"Authorization": f"Bearer {X_BEARER}"}
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": query,
        "max_results": 100,
        "tweet.fields": "id,text,author_id,created_at,lang"
    }
    if next_token:
        params["next_token"] = next_token
    
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 429:
            print("Rate limit reached. Waiting 60 seconds...")
            time.sleep(60)
            return collect_x(query, next_token)
        elif r.status_code != 200:
            print(f"Error {r.status_code}: {r.text}")
            return {"data": [], "meta": {}}
        return r.json()
    except Exception as e:
        print(f"Request error: {e}")
        return {"data": [], "meta": {}}

# ==================== Collect & Batch Insert =================
def collect_data_fast(max_requests=100):
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS whoosh_raw (
        id SERIAL PRIMARY KEY,
        tweet_id VARCHAR(50) UNIQUE,
        author_id VARCHAR(50),
        created_at TIMESTAMP,
        text TEXT,
        lang VARCHAR(10)
    );
    """)
    conn.commit()

    total_collected = 0
    request_count = 0
    
    for kw in KEYWORDS:
        if request_count >= max_requests:
            break

        for lang in LANGUAGES:
            if request_count >= max_requests:
                break

            query = f"{kw} lang:{lang} -is:retweet"
            next_token = None
            while request_count < max_requests:
                request_count += 1
                data = collect_x(query, next_token)
                tweets = data.get("data", [])

                if not tweets:
                    print(f"No more tweets for '{kw}' lang:{lang}")
                    break

                records = [(t["id"], t["author_id"], t["created_at"], t["text"], t["lang"]) for t in tweets]

                try:
                    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in records)
                    cur.execute(
                        f"""
                        INSERT INTO whoosh_raw (tweet_id, author_id, created_at, text, lang)
                        VALUES {args_str}
                        ON CONFLICT (tweet_id) DO NOTHING;
                        """
                    )
                    conn.commit()
                    total_collected += len(tweets)
                    print(f"Inserted {len(tweets)} tweets. Total collected: {total_collected}")
                except Exception as e:
                    print(f"Insert error: {e}")
                    conn.rollback()

                next_token = data.get("meta", {}).get("next_token")
                if not next_token:
                    break

                time.sleep(DELAY)

    cur.close()
    conn.close()
    return f"Collected approximately {total_collected} tweets in {request_count} requests."

# ==================== Preprocessing ====================
def preprocess():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS whoosh_clean (
        id SERIAL PRIMARY KEY,
        tweet_id VARCHAR(50) UNIQUE,
        text_original TEXT,
        text_clean TEXT,
        text_hash VARCHAR(64),
        lang VARCHAR(10),
        created_at TIMESTAMP
    );
    """)
    conn.commit()

    df = pd.read_sql("SELECT * FROM whoosh_raw", conn)

    if df.empty:
        conn.close()
        return "No data to preprocess."

    def clean_text(text):
        if pd.isna(text):
            return ""
        text = re.sub(r'http\S+', '', text)
        text = re.sub(r'@\w+', '', text)
        text = re.sub(r'#', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip().lower()

    df["text_clean"] = df["text"].apply(clean_text)
    df["text_hash"] = df["text_clean"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())

    df_dedup = df.drop_duplicates(subset=["text_hash"], keep="first")
    removed = len(df) - len(df_dedup)

    for _, row in df_dedup.iterrows():
        try:
            cur.execute("""
                INSERT INTO whoosh_clean (tweet_id, text_original, text_clean, text_hash, lang, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tweet_id) DO NOTHING;
            """, (row["tweet_id"], row["text"], row["text_clean"], row["text_hash"], row["lang"], row["created_at"]))
        except Exception:
            pass

    conn.commit()
    cur.close()
    conn.close()
    return f"Preprocessed {len(df_dedup)} tweets. Removed {removed} duplicates."

# ==================== Analysis ====================
def analyze():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM whoosh_clean", conn)

    if df.empty:
        conn.close()
        return {"error": "No data to analyze."}

    positive_words = [
        "bagus", "cepat", "nyaman", "canggih", "modern", "hebat", "keren",
        "mantap", "good", "great", "amazing", "excellent", "wow", "asyik",
        "enak", "murah", "lancar", "top", "puas", "terbaik", "memuaskan",
        "mantul", "recommended", "oke", "suka", "senang", "happy",
        "efisien", "ramah", "beres", "kenceng", "responsif"
    ]

    negative_words = [
        "lambat", "buruk", "jelek", "mahal", "rugi", "bad", "slow",
        "expensive", "poor", "terrible", "anjir", "anjing", "goblok",
        "parah", "macet", "ngebug", "gagal", "kecewa", "mengecewakan",
        "tolol", "bodoh", "error", "lemot", "bete", "nyebelin", "sampah",
        "zonk", "anj", "bangsat", "tai", "ngaco", "ngelek", "jelek banget",
        "aneh", "ironis", "lucu", "katanya", "masa", "malah"
    ]

    intensifiers = ["banget", "sekali", "parah", "amat", "sangat", "terlalu", "bgt"]
    negations = ["tidak", "nggak", "gak", "bukan", "tak", "ndak", "ga"]
    contrastive_words = ["tapi", "namun", "padahal", "meski", "walau"]
    sarcasm_markers = ["?", "!", "masa", "katanya", "lah", "kok", "malah"]

    def simple_sentiment(text):
        text_low = text.lower()
        words = text_low.split()
        score = 0

        for i, word in enumerate(words):
            if word in positive_words:
                local_score = 0.8
                if i > 0 and words[i - 1] in negations:
                    local_score *= -1.5
                if i + 1 < len(words) and words[i + 1] in intensifiers:
                    local_score *= 1.4
                score += local_score

            elif word in negative_words:
                local_score = -1.5
                if i > 0 and words[i - 1] in negations:
                    local_score *= -1.0
                if i + 1 < len(words) and words[i + 1] in intensifiers:
                    local_score *= 1.5
                score += local_score

        # Jika mengandung tanda tanya atau kata seperti "masa", "katanya" bersama kata negatif → sarcasm
        if any(s in text_low for s in sarcasm_markers) and any(n in text_low for n in negative_words):
            score -= 1.0

        # Frasa kontras
        for cword in contrastive_words:
            if cword in words:
                after = words[words.index(cword) + 1 :]
                for w in after:
                    if w in positive_words:
                        score += 0.5
                    elif w in negative_words:
                        score -= 1.0

        if score > 0.5:
            return "positive"
        elif score < -0.3:
            return "negative"
        else:
            return "neutral"

    df["sentiment"] = df["text_clean"].apply(simple_sentiment)

    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS whoosh_analysis (
        id SERIAL PRIMARY KEY,
        tweet_id VARCHAR(50) UNIQUE,
        text_clean TEXT,
        sentiment VARCHAR(20),
        lang VARCHAR(10),
        created_at TIMESTAMP
    );
    """)
    conn.commit()

    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO whoosh_analysis (tweet_id, text_clean, sentiment, lang, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tweet_id) DO UPDATE SET sentiment = EXCLUDED.sentiment;
            """, (row["tweet_id"], row["text_clean"], row["sentiment"], row["lang"], row["created_at"]))
        except Exception:
            pass

    conn.commit()
    cur.close()

    summary = df["sentiment"].value_counts().to_dict()
    conn.close()
    return summary




# ==================== Load Kaggle Dataset ====================
def load_kaggle_data():
    import os
    import kagglehub
    import pandas as pd

    try:
        conn = get_conn()
        cur = conn.cursor()

        # Pastikan tabel sudah siap
        cur.execute("""
        CREATE TABLE IF NOT EXISTS kaggle_whoosh (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP,
            text TEXT,
            sentiment VARCHAR(20),
            lang VARCHAR(10)
        );
        """)
        conn.commit()

        # Unduh dataset dari Kaggle
        dataset_path = kagglehub.dataset_download("gevabriel/whoosh")

        # Cari file CSV dalam folder dataset
        csv_files = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError("No CSV file found in Kaggle dataset folder.")

        file_path = os.path.join(dataset_path, csv_files[0])
        df = pd.read_csv(file_path)

        # Pastikan kolom "tweet" dan "sentiment" ada
        if "tweet" not in df.columns or "sentiment" not in df.columns:
            raise ValueError(f"Dataset must contain 'tweet' and 'sentiment' columns. Found columns: {df.columns.tolist()}")

        # Rename agar seragam dengan pipeline kita
        df.rename(columns={"tweet": "text"}, inplace=True)

        # Tambahkan kolom pendukung jika belum ada
        if "created_at" not in df.columns:
            df["created_at"] = pd.Timestamp.now()
        if "lang" not in df.columns:
            df["lang"] = "id"

        # Masukkan ke PostgreSQL
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO kaggle_whoosh (created_at, text, sentiment, lang)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (row["created_at"], row["text"], row["sentiment"], row["lang"]))

        conn.commit()
        cur.close()
        conn.close()

        return f"Successfully loaded {len(df)} Kaggle rows into database."

    except Exception as e:
        return f"Failed to load dataset: {e}"
