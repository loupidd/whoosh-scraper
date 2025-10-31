import requests
import psycopg2
import pandas as pd
import hashlib
import time
import re
from collections import Counter
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

    positive_words = ['bagus', 'cepat', 'nyaman', 'canggih', 'modern', 'hebat', 'keren', 'mantap', 'good', 'great', 'amazing', 'excellent']
    negative_words = ['lambat', 'buruk', 'jelek', 'mahal', 'rugi', 'bad', 'slow', 'expensive', 'poor', 'terrible']

    def simple_sentiment(text):
        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        if pos_count > neg_count:
            return "positive"
        elif neg_count > pos_count:
            return "negative"
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
