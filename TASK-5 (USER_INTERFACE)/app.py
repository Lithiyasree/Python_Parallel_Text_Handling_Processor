import streamlit as st
import os
import json
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter

from database import get_connection
from database import init_db
from loader import parallel_process
from rule_engine import apply_rules
from analytics_module import get_data
from export_module import export_pdf
from email_service import send_email
from search import search_keyword, search_regex
from clear_records import clear_all_records

# -------------- Initialize DB -----------
init_db()

st.set_page_config(page_title="Parallel Text Processor", layout="wide")


# SIDEBAR 
st.sidebar.markdown("## Parallel Text Processor")
st.sidebar.markdown("---")

menu = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "Upload Files",
        "Run Pipeline",
        "View Records",
        "Search",
        "Analytics",
        "Email Report",
        "Clear Data"
    ]
)



# MAIN HEADER SECTION 

section_icons = {
    "Overview": "🎯",
    "Upload Files": "📂",
    "Run Pipeline": "⚙",
    "View Records": "📑",
    "Search": "🔍",
    "Analytics": "📈",
    "Email Report": "📧",
    "Clear Data":"📤"
}

section_descriptions = {
    "Overview": "View system statistics, stored chunks, and overall performance metrics.",
    "Upload Files": "Upload multiple text, csv files to begin processing and chunk generation.",
    "Run Pipeline": "Create chunks using parallel processing and apply rule-based scoring.",
    "View Records": "Browse, filter and download all processed text records.",
    "Search": "Search stored chunks using keyword or regex-based queries.",
    "Analytics": "Visualize score and sentiment distribution",
    "Email Report": "Generate a PDF report and send results via email.",
    "Clear Data": "Permanently delete all stored records from the database."
}

st.markdown("# Python Parallel Text Processor")
st.markdown(f"## {section_icons.get(menu, '')} {menu}")
st.markdown(
f"""
<div style="
    background-color:#1E3A8A;
    padding:12px 18px;
    border-radius:8px;
    color:white;
    font-size:15px;
    margin-top:5px;
    margin-bottom:15px;
">
    {section_descriptions.get(menu, "")}
</div>
""",
unsafe_allow_html=True
)
st.divider()

#----------------- OVERVIEW FUNCTIONS ----------------

def get_total_csv_records():
    total_records = 0
    if os.path.exists("data"):
        for file in os.listdir("data"):
            if file.endswith(".csv"):
                path = os.path.join("data", file)

                try:
                    df = pd.read_csv(path)
                    total_records += len(df)
                except:
                    pass

    return total_records

def get_overview_metrics():
    conn = get_connection()
    cursor = conn.cursor()

    # Total Chunks
    cursor.execute("SELECT COUNT(*) FROM chunks")
    total_chunks = cursor.fetchone()[0]

    # Average Score
    cursor.execute("SELECT AVG(score) FROM chunks")
    avg_score = cursor.fetchone()[0]
    avg_score = round(avg_score, 2) if avg_score else 0

    # Unique UIDs
    cursor.execute("SELECT COUNT(DISTINCT uid) FROM chunks")
    unique_uids = cursor.fetchone()[0]

    # Top Rule
    cursor.execute("""
        SELECT matched_rules
        FROM chunks
        WHERE matched_rules IS NOT NULL
        AND matched_rules != ''
    """)

    rows = cursor.fetchall()

    all_rules = []
    for (rule_str,) in rows:
        split_rules = [r.strip() for r in rule_str.split(",") if r.strip()]
        all_rules.extend(split_rules)

    if all_rules:
        top_rule = Counter(all_rules).most_common(1)[0][0]
    else:
        top_rule = "N/A"

    # Sentiment counts
    cursor.execute("SELECT COUNT(*) FROM chunks WHERE sentiment='Positive'")
    positive = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE sentiment='Negative'")
    negative = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE sentiment='Neutral'")
    neutral = cursor.fetchone()[0]

    conn.close()

    return total_chunks, avg_score, unique_uids, top_rule, positive, negative, neutral


# ---------------- OVERVIEW ----------------
if menu == "Overview":

    total_chunks, avg_score, unique_uids, top_rule, positive, negative, neutral = get_overview_metrics()
    total_csv_records = get_total_csv_records()

    # Processing Summary 
    st.markdown("### Processing Summary")

    overview_table = f"""
<table style="width:100%; border-collapse: collapse;">
<tr>
    <th>Total CSV Records</th>
    <th>Chunks Stored</th>
    <th>Average Score</th>
    <th>Unique IDs</th>
    <th>Top Rule</th>
</tr>
<tr>
    <td><b>{total_csv_records}</b></td>
    <td><b>{total_chunks}</b></td>
    <td><b>{round(avg_score,2)}</b></td>
    <td><b>{unique_uids}</b></td>
    <td><b>{top_rule}</b></td>
</tr>
</table>
"""

    st.markdown(overview_table, unsafe_allow_html=True)


    # Sentiment Summary
    st.markdown("### Sentiment Analysis Summary")

    sentiment_table = f"""
    <table style="width:100%; border-collapse: collapse;">
        <tr>
            <th>🟢 Positive Reviews</th>
            <th>⚪ Neutral Reviews</th>
            <th>🔴 Negative Reviews</th>   
        </tr>
        <tr>
            <td><b>{positive}</b></td>
            <td><b>{neutral}</b></td>
            <td><b>{negative}</b></td>
        </tr>
    </table>
    """

    st.markdown(sentiment_table, unsafe_allow_html=True)
    
# ---------------- UPLOAD ----------------
if menu == "Upload Files":

    st.header("Step 1: Upload Files")

    uploaded_files = st.file_uploader(
        "Upload TXT or CSV Files",
        type=["txt", "csv"],
        accept_multiple_files=True
    )

    os.makedirs("data", exist_ok=True)

    if uploaded_files:

        for file in uploaded_files:
            file_path = os.path.join("data", file.name)
            with open(file_path, "wb") as f:
                f.write(file.getbuffer())

        st.success("Files Uploaded Successfully ✅")

    st.subheader("Saved Files:")
    st.write(os.listdir("data"))

    st.info("Next Go to 'Run Pipeline' to process the uploaded files.")



# ---------------- RUN PIPELINE ----------------
if menu == "Run Pipeline":

    st.header("Step 2: Process Files")

    files = [f"data/{f}" for f in os.listdir("data")]

    if not files:
        st.warning("No files found. Please upload files first.")
        st.stop()

    st.write("Files Ready for Processing:")
    st.write(os.listdir("data"))

    # Detect CSV files
    csv_files = [f for f in files if f.endswith(".csv")]
    selected_column = None

    if csv_files:
        st.subheader("CSV Column Selection")

        df_preview = pd.read_csv(csv_files[0])
        st.write("Preview of first CSV file:")
        st.dataframe(df_preview.head())

        selected_column = st.selectbox(
            "Select column to process (applies to all CSV files)",
            df_preview.columns
        )

    # Settings
    st.subheader("Processing Settings")

    max_workers = st.slider("Max Workers (Parallel Threads)", 1, 10, 4)
    group_size = st.slider("Words per Chunk", 50, 500, 100)

    if st.button("Start Processing"):

        

        success = parallel_process(
            files,
            selected_column,
            group_size,
            max_workers
        )

        if success:
            st.success("Chunks Created ✅")
        else:
            st.error("Some files failed to process ❌")

    st.markdown("---")

    st.header("Step 3: Apply Scoring")

    if st.button("Apply Rule Scoring"):
        apply_rules()
        st.success("Scoring Completed ✅")


# ---------------- VIEW RECORDS ----------------

if menu == "View Records":

    conn = get_connection()
    cursor = conn.cursor()

    # Total records count
    cursor.execute("SELECT COUNT(*) FROM chunks")
    total_records = cursor.fetchone()[0]

    st.write(f"Total Records: {total_records}")

    # Pagination settings
    page_size = 1000
    total_pages = max((total_records // page_size) + 1, 1)

    page = st.number_input(
        "Page Number",
        min_value=1,
        max_value=total_pages,
        value=1
    )

    offset = (page - 1) * page_size

    # Query only required rows
    query = f"""
    SELECT * FROM chunks
    LIMIT {page_size} OFFSET {offset}
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:

        # Sentiment color function
        def color_sentiment(val):
            if val == "Positive":
                return "color: green; font-weight: bold"
            elif val == "Negative":
                return "color: red; font-weight: bold"
            else:
                return "color: gray; font-weight: bold"

        # Apply styling
        styled_df = df.style.map(
            color_sentiment,
            subset=["sentiment"]
        )

        st.dataframe(styled_df, use_container_width=True)

        # Download button
        st.download_button(
            "Download CSV",
            df.to_csv(index=False),
            file_name=f"records_{page}.csv"
        )

    else:
        st.warning("No records found.")

# ---------------- ANALYTICS ----------------

if menu == "Analytics":

    df = get_data()

    if not df.empty:

        st.markdown("## 📊 Analytics Dashboard")

        total_chunks, avg_score, unique_uids, top_rule, positive, negative, neutral = get_overview_metrics()

        col1, col2 = st.columns(2)

        # PIE CHART 
        with col1:

            st.subheader("Sentiment Distribution")

            labels = ["Positive", "Negative", "Neutral"]
            sizes = [positive, negative, neutral]

            fig1, ax1 = plt.subplots(figsize=(5,4))

            ax1.pie(
                sizes,
                labels=labels,
                autopct="%1.0f%%",
                colors=["#10F759", "#EF4444", "#6E08EA"],
                startangle=90
            )

            ax1.set_aspect("equal")

            st.pyplot(fig1, use_container_width=True)

            plt.close(fig1)

        # HISTOGRAM 
        with col2:

            st.subheader("Score Histogram")

            fig2, ax2 = plt.subplots(figsize=(5,4))

            ax2.hist(
                df["score"],
                bins=10,
                color="#3B82F6",
                edgecolor="white"
            )

            ax2.set_xlabel("Score")
            ax2.set_ylabel("Frequency")
            ax2.set_title("Score Distribution")

            st.pyplot(fig2, use_container_width=True)

            plt.close(fig2)

        st.markdown("---")

    else:
        st.warning("No Data Available")

# ---------------- SEARCH ----------------
if menu == "Search":

    st.header("Search Records")

    mode = st.radio("Search Mode", ["Keyword", "Regex"])

    query = st.text_input("Enter Search Query")

    limit = st.slider("Max Results", 1, 100, 20)

    if st.button("Search"):

        if not query:
            st.warning("Please enter a search query")
            st.stop()

        if mode == "Keyword":
            result = search_keyword(query, limit)
        else:
            result = search_regex(query, limit)

        if result.empty:
            st.info("No results found")
        else:
            st.dataframe(result, use_container_width=True)

            st.download_button(
                "Download Search Results",
                result.to_csv(index=False),
                file_name="search_results.csv"
            )

# ---------------- EMAIL REPORT ----------------

if menu == "Email Report":

        recipient = st.text_input("Recipient Email")

        if st.button("Generate & Send Report"):

            if not recipient:
                st.warning("Please enter a recipient email.")
                st.stop()

            os.makedirs("report", exist_ok=True)

            # CSV
            conn = get_connection()
            df_full = pd.read_sql_query("SELECT * FROM chunks", conn)
            conn.close()

            if df_full.empty:
                st.warning("No records available.")
                st.stop()

            max_csv_rows = 20000
            if len(df_full) > max_csv_rows:
                st.warning("CSV too large. Sending only first 10,000 rows.")
                df_csv = df_full.head(max_csv_rows)
            else:
                df_csv = df_full

            csv_path = "report/view_records_report.csv"
            df_csv.to_csv(csv_path, index=False)

            # PDF
            total_chunks, avg_score, unique_uids, top_rule, positive, negative, neutral = get_overview_metrics()
            total_csv_records = get_total_csv_records()
            data = get_data()
            
            pdf_path = export_pdf(
                data,                
                total_csv_records, 
                total_chunks,
                avg_score,
                unique_uids,
                positive,
                negative,
                neutral
            )

            # Email body
            body = f"""
    Parallel Text Processing Report

    Total CSV Records: {total_csv_records}
    Chunks Stored: {total_chunks}
    Average Score: {avg_score}
    Unique UIDs: {unique_uids}
    Top Rule: {top_rule}

    Sentiment Counts:
    Positive: {positive}
    Negative: {negative}
    Neutral: {neutral}
    """

            # Send Email 
            send_email(
                recipient,
                "Parallel Text Processing Report",
                body,
                attachments=[pdf_path, csv_path]
            )

            st.success("✅ Report Sent Successfully with CSV & PDF")


# ---------------- CLEAR RECORDS ----------------
if menu == "Clear Data":

    st.warning("⚠ This will permanently delete all records.")

    confirm = st.checkbox("I confirm that I want to delete all records.")

    if st.button("🗑 Clear All Records"):

        if confirm:
            clear_all_records()
            st.success("All records deleted successfully. ✅")
        else:
            st.error("Please confirm before deleting.")







































# RUN APP: streamlit run app.py































