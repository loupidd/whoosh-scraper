import streamlit as st
import pandas as pd
import psycopg2
from whoosh_pipeline import collect_data_fast, preprocess, analyze, get_conn

st.set_page_config(page_title="Whoosh Twitter Analyzer", layout="wide")

st.title("Whoosh Twitter Data Collector & Analyzer")

menu = st.sidebar.selectbox(
    "Select Action",
    ["Collect Data", "Preprocess Data", "Analyze Sentiment", "View Data"]
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

# =================== VIEW DATA ===================
elif menu == "View Data":
    st.header("View Collected, Cleaned, and Analyzed Data")

    conn = get_conn()

    tab1, tab2, tab3 = st.tabs(["Raw Data", "Cleaned Data", "Analyzed Data"])

    with tab1:
        st.subheader("whoosh_raw (Collected Tweets)")
        try:
            df_raw = pd.read_sql("SELECT * FROM whoosh_raw ORDER BY created_at DESC LIMIT 500", conn)
            st.write(f"Total rows: {len(df_raw)}")
            st.dataframe(df_raw)
        except Exception as e:
            st.error(f"Error reading whoosh_raw: {e}")

    with tab2:
        st.subheader("whoosh_clean (After Preprocessing)")
        try:
            df_clean = pd.read_sql("SELECT * FROM whoosh_clean ORDER BY created_at DESC LIMIT 500", conn)
            st.write(f"Total rows: {len(df_clean)}")
            st.dataframe(df_clean)
        except Exception as e:
            st.error(f"Error reading whoosh_clean: {e}")

    with tab3:
        st.subheader("whoosh_analysis (After Sentiment Analysis)")
        try:
            df_analysis = pd.read_sql("SELECT * FROM whoosh_analysis ORDER BY created_at DESC LIMIT 500", conn)
            st.write(f"Total rows: {len(df_analysis)}")
            st.dataframe(df_analysis)

            if not df_analysis.empty:
                st.subheader("Sentiment Distribution")
                sentiment_count = df_analysis["sentiment"].value_counts().reset_index()
                sentiment_count.columns = ["Sentiment", "Count"]
                st.bar_chart(sentiment_count.set_index("Sentiment"))
        except Exception as e:
            st.error(f"Error reading whoosh_analysis: {e}")

    conn.close()
