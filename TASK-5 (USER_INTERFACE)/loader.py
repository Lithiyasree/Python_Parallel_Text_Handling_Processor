from concurrent.futures import ThreadPoolExecutor
from database import get_connection
import uuid
import pandas as pd
import os

# SPLIT TEXT 
def split_text(text, size):
    words = text.split()
    for i in range(0, len(words), size):
        yield " ".join(words[i:i+size])

def save_chunks(text, group_size):
    conn = get_connection()
    cursor = conn.cursor()

    uid = str(uuid.uuid4())
    batch = []

    for chunk in split_text(text, group_size):
        batch.append((uid, chunk))

        if len(batch) >= 5000:
            cursor.executemany(
                "INSERT INTO chunks (uid, chunk) VALUES (?, ?)",
                batch
            )
            batch = []

    if batch:
        cursor.executemany(
            "INSERT INTO chunks (uid, chunk) VALUES (?, ?)",
            batch
        )

    conn.commit()
    conn.close()


# PROCESS TXT FILE 
def process_txt_file(filepath, group_size):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            text = f.read()

        save_chunks(text, group_size)
        return True
    except Exception as e:
        print("TXT Processing Error:", e)
        return False


# PROCESS CSV FILE
def process_csv_file(filepath, selected_column, group_size):
    try:
        df = pd.read_csv(filepath)

        if selected_column not in df.columns:
            return False

        # Convert column to text and combine
        text = " ".join(df[selected_column].dropna().astype(str))

        save_chunks(text, group_size)
        return True

    except Exception as e:
        print("CSV Processing Error:", e)
        return False

def parallel_process(files, selected_column, group_size, max_workers=4):

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = []

        for filepath in files:

            if filepath.endswith(".txt"):
                futures.append(
                    executor.submit(process_txt_file, filepath, group_size)
                )

            elif filepath.endswith(".csv"):
                futures.append(
                    executor.submit(process_csv_file, filepath, selected_column, group_size)
                )

        for future in futures:
            results.append(future.result())

    return all(results)