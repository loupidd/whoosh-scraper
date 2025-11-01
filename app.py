import streamlit as st
import pandas as pd
import psycopg2
from whoosh_pipeline import collect_data_fast, preprocess, analyze, get_conn, load_kaggle_data

st.set_page_config(page_title="Whoosh Twitter & Kaggle Analyzer", layout="wide")

st.title("Whoosh Data Collector & Sentiment Analyzer")

menu = st.sidebar.selectbox(
    "Select Action",
    ["Collect Data", "Preprocess Data", "Analyze Sentiment", "Load Kaggle Dataset", "View Data"]
)

# =================== COLLECT DATA ===================
if menu == "Collect Data":
    st.header("Collect Tweets from X (Twitter)")
    st.write("This will collect tweets containing keywords related to Kereta Cepat Whoosh.")
    
    if st.button("Start Collecting"):
        with st.spinner("Collecting tweets..."):
            result = collect_data_fast()
        st.success(result)

# =================== PREPROCESS DATA ===================
elif menu == "Preprocess Data":
    st.header("Clean and Deduplicate Tweets")
    st.write("Removes duplicate tweets and cleans text content.")
    
    if st.button("Start Preprocessing"):
        with st.spinner("Processing data..."):
            result = preprocess()
        st.success(result)

# =================== ANALYZE SENTIMENT ===================
elif menu == "Analyze Sentiment":
    st.header("Analyze Sentiment of Tweets")
    st.write("Performs basic sentiment analysis using keyword matching.")
    
    if st.button("Run Sentiment Analysis"):
        with st.spinner("Analyzing sentiments..."):
            result = analyze()
        st.write("Sentiment summary:")
        st.json(result)

# =================== LOAD KAGGLE DATASET ===================
elif menu == "Load Kaggle Dataset":
    st.header("Load External Kaggle Dataset")
    st.write("This loads the Whoosh dataset from Kaggle to complement Twitter data (to reach ~5000 rows).")
    
    if st.button("Load Dataset"):
        with st.spinner("Loading dataset from Kaggle..."):
            result = load_kaggle_data()
            st.success(result)

# =================== VIEW DATA ===================
elif menu == "View Data":
    st.header("View Collected, Cleaned, and Analyzed Data")

    conn = get_conn()

    tab1, tab2, tab3, tab4 = st.tabs(["Raw Data (Twitter)", "Cleaned Data", "Analyzed Data", "Kaggle Dataset"])

    # ---- RAW DATA ----
    with tab1:
        st.subheader("whoosh_raw (Collected Tweets)")
        try:
            df_raw = pd.read_sql("SELECT * FROM whoosh_raw ORDER BY created_at DESC LIMIT 5000", conn)
            st.write(f"Total rows: {len(df_raw)}")
            st.dataframe(df_raw)
        except Exception as e:
            st.error(f"Error reading whoosh_raw: {e}")

    # ---- CLEANED DATA ----
    with tab2:
        st.subheader("whoosh_clean (After Preprocessing)")
        try:
            df_clean = pd.read_sql("SELECT * FROM whoosh_clean ORDER BY created_at DESC LIMIT 5000", conn)
            st.write(f"Total rows: {len(df_clean)}")
            st.dataframe(df_clean)
        except Exception as e:
            st.error(f"Error reading whoosh_clean: {e}")

    # ---- ANALYZED DATA ----
    with tab3:
        st.subheader("whoosh_analysis (After Sentiment Analysis)")
        try:
            df_analysis = pd.read_sql("SELECT * FROM whoosh_analysis ORDER BY created_at DESC LIMIT 5000", conn)
            st.write(f"Total rows: {len(df_analysis)}")
            st.dataframe(df_analysis)

            if not df_analysis.empty:
                st.subheader("Sentiment Distribution (Twitter)")
                sentiment_count = df_analysis["sentiment"].value_counts().reset_index()
                sentiment_count.columns = ["Sentiment", "Count"]
                st.bar_chart(sentiment_count.set_index("Sentiment"))
        except Exception as e:
            st.error(f"Error reading whoosh_analysis: {e}")

    # ---- KAGGLE DATA ----
    with tab4:
        st.subheader("kaggle_whoosh (External Dataset)")
        try:
            df_kaggle = pd.read_sql("SELECT * FROM kaggle_whoosh ORDER BY created_at DESC LIMIT 5000", conn)
            st.write(f"Total rows: {len(df_kaggle)}")
            st.dataframe(df_kaggle)

            if "sentiment" in df_kaggle.columns and not df_kaggle.empty:
                st.subheader("Sentiment Distribution (Kaggle Dataset)")
                sentiment_count_kaggle = df_kaggle["sentiment"].value_counts().reset_index()
                sentiment_count_kaggle.columns = ["Sentiment", "Count"]
                st.bar_chart(sentiment_count_kaggle.set_index("Sentiment"))
        except Exception as e:
            st.error(f"Error reading kaggle_whoosh: {e}")

    conn.close()
