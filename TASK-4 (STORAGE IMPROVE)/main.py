import csv
import sqlite3
import re
import time
from datetime import datetime


# START TIMER (HIGH PRECISION)

overall_start = time.perf_counter()


# TEXT PREPROCESSING

def preprocess_text(text):
    return re.findall(r"\b\w+\b", text.lower())


# KEYWORD DICTIONARIES

quality_keywords = {
    "good": 1,
    "excellent": 2,
    "durable": 2,
    "cheap": -1,
    "poor": -2
}

price_keywords = {
    "worth": 2,
    "affordable": 2,
    "expensive": -1,
    "overpriced": -2
}

delivery_keywords = {
    "fast": 1,
    "ontime": 2,
    "late": -1,
    "delayed": -2
}

packaging_keywords = {
    "packed": 1,
    "damaged": -2,
    "broken": -2
}

performance_keywords = {
    "perfectly": 2,
    "smooth": 2,
    "slow": -1,
    "working": 1
}


# SCORE FUNCTION

def calculate_score(words, keyword_dict):
    score = 0
    for word in words:
        if word in keyword_dict:
            score += keyword_dict[word]
    return score


def process_review(text):
    words = preprocess_text(text)

    quality_score = calculate_score(words, quality_keywords)
    price_score = calculate_score(words, price_keywords)
    delivery_score = calculate_score(words, delivery_keywords)
    packaging_score = calculate_score(words, packaging_keywords)
    performance_score = calculate_score(words, performance_keywords)

    total_score = (
        quality_score + price_score + delivery_score +
        packaging_score + performance_score
    )

    if total_score > 0:
        sentiment = "Positive"
    elif total_score < 0:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return sentiment, total_score, timestamp


# LOAD CSV (UP TO 1 MILLION)

print("Loading CSV...")

reviews = []

with open(r"C:\Users\rlith\OneDrive\Desktop\INFOSYS SPRINGBOARD INTERN\DATASET\test.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        if i >= 1000000:
            break
        reviews.append(row)

print("Total Records Loaded:", len(reviews))


# DATABASE SETUP

conn = sqlite3.connect("amazon-product-reviews.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT,
    actual_label TEXT,
    predicted_sentiment TEXT,
    total_score REAL,
    timestamp TEXT
)
""")

conn.commit()


# INSERT DATA (BULK INSERT)

print("Processing and Inserting Data...")

insert_start = time.perf_counter()

processed_data = []

for row in reviews:
    review_text = row["Review"]
    actual_label = row["Label"]

    predicted_sentiment, total_score, timestamp = process_review(review_text)

    processed_data.append((
        review_text,
        actual_label,
        predicted_sentiment,
        total_score,
        timestamp
    ))

print("Total Processed Records:", len(processed_data))

cursor.executemany("""
INSERT INTO results 
(text, actual_label, predicted_sentiment, total_score, timestamp)
VALUES (?, ?, ?, ?, ?)
""", processed_data)

conn.commit()

insert_end = time.perf_counter()
print("Insert Time:", round(insert_end - insert_start, 2), "seconds")


# QUERY BEFORE INDEX

query1_start = time.perf_counter()

cursor.execute("""
SELECT predicted_sentiment, COUNT(*) 
FROM results 
GROUP BY predicted_sentiment
""")

print("Query Result:", cursor.fetchall())

query1_end = time.perf_counter()
before_time = query1_end - query1_start

print("Query Time (Before Index):", round(before_time, 6), "seconds")


# ADD INDEXES (OPTIMIZATION)

print("Applying Indexes...")

cursor.execute("CREATE INDEX IF NOT EXISTS idx_predicted_sentiment ON results(predicted_sentiment)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_total_score ON results(total_score)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON results(timestamp)")

conn.commit()


# QUERY AFTER INDEX

query2_start = time.perf_counter()

cursor.execute("""
SELECT predicted_sentiment, COUNT(*) 
FROM results 
GROUP BY predicted_sentiment
""")

print("Query Result:", cursor.fetchall())

query2_end = time.perf_counter()
after_time = query2_end - query2_start

print("Query Time (After Index):", round(after_time, 6), "seconds")


# PERFORMANCE COMPARISON

if before_time > 0:
    improvement = ((before_time - after_time) / before_time) * 100
    print("\nPerformance Improvement:", round(improvement, 2), "%")
else:
    print("\nPerformance Improvement: Cannot calculate (Before time is 0)")
    improvement = 0


# CLOSE CONNECTION

conn.close()

overall_end = time.perf_counter()

print("\nTotal Execution Time:", round(overall_end - overall_start, 2), "seconds")
print("Process Completed Successfully")







# FOR 4 Million Records

# PS C:\Users\rlith\OneDrive\Desktop\INFOSYS SPRINGBOARD INTERN\TASK\TASK-4 (STORAGE IMPROVE)> python main.py
# Loading CSV...
# Total Records Loaded: 400000
# Processing and Inserting Data...
# Total Processed Records: 400000
# Insert Time: 36.28 seconds
# Query Result: [('Negative', 27472), ('Neutral', 245739), ('Positive', 126789)]
# Query Time (Before Index): 0.682761 seconds
# Applying Indexes...
# Query Result: [('Negative', 27472), ('Neutral', 245739), ('Positive', 126789)]
# Query Time (After Index): 0.055388 seconds

# Performance Improvement: 91.89 %

# Total Execution Time: 43.67 seconds
# Process Completed Successfully











# import csv
# import sqlite3
# import re
# import time
# from datetime import datetime


# # START TIMER (USE PERF_COUNTER)

# overall_start = time.perf_counter()


# # TEXT PREPROCESSING

# def preprocess_text(text):
#     return re.findall(r"\b\w+\b", text.lower())


# # KEYWORD DICTIONARIES

# quality_keywords = {
#     "good": 1, "excellent": 2, "durable": 2,
#     "cheap": -1, "poor": -2
# }

# price_keywords = {
#     "worth": 2, "affordable": 2,
#     "expensive": -1, "overpriced": -2
# }

# delivery_keywords = {
#     "fast": 1, "ontime": 2,
#     "late": -1, "delayed": -2
# }

# packaging_keywords = {
#     "packed": 1,
#     "damaged": -2, "broken": -2
# }

# performance_keywords = {
#     "perfectly": 2, "smooth": 2,
#     "slow": -1, "working": 1
# }


# # SCORE FUNCTIONS

# def calculate_score(words, keyword_dict):
#     score = 0
#     for word in words:
#         if word in keyword_dict:
#             score += keyword_dict[word]
#     return score


# def process_review(text):
#     words = preprocess_text(text)

#     quality_score = calculate_score(words, quality_keywords)
#     price_score = calculate_score(words, price_keywords)
#     delivery_score = calculate_score(words, delivery_keywords)
#     packaging_score = calculate_score(words, packaging_keywords)
#     performance_score = calculate_score(words, performance_keywords)

#     total_score = (
#         quality_score + price_score + delivery_score +
#         packaging_score + performance_score
#     )

#     if total_score > 0:
#         sentiment = "Positive"
#     elif total_score < 0:
#         sentiment = "Negative"
#     else:
#         sentiment = "Neutral"

#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#     return (
#         text,
#         "Yes" if quality_score > 0 else "No",
#         "Yes" if price_score > 0 else "No",
#         "Yes" if delivery_score > 0 else "No",
#         "Yes" if packaging_score > 0 else "No",
#         "Yes" if performance_score > 0 else "No",
#         total_score,
#         sentiment,
#         timestamp
#     )

# # LOAD CSV

# print("Loading CSV...")

# with open("amazon-product-reviews.csv", "r", encoding="utf-8") as file:
#     reader = csv.DictReader(file)
#     reviews = list(reader)[:1000000]

# print("Total Records Loaded:", len(reviews))


# # DATABASE SETUP

# conn = sqlite3.connect("amazon-product-reviews.db")
# cursor = conn.cursor()

# cursor.execute("""
# CREATE TABLE IF NOT EXISTS results (
#     id INTEGER PRIMARY KEY AUTOINCREMENT,
#     text TEXT,
#     quality_status TEXT,
#     price_status TEXT,
#     delivery_status TEXT,
#     packaging_status TEXT,
#     performance_status TEXT,
#     total_score REAL,
#     overall_sentiment TEXT,
#     timestamp TEXT
# )
# """)

# conn.commit()


# # BULK INSERT

# print("Processing and Inserting Data...")

# insert_start = time.perf_counter()

# processed_data = []

# for row in reviews:
#     if "reviews.text" in row:
#         result = process_review(row["reviews.text"])
#         processed_data.append(result)

# cursor.executemany("""
# INSERT INTO results 
# (text, quality_status, price_status, delivery_status, packaging_status,
#  performance_status, total_score, overall_sentiment, timestamp)
# VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
# """, processed_data)

# conn.commit()

# insert_end = time.perf_counter()
# print("Insert Time:", round(insert_end - insert_start, 4), "seconds")


# # QUERY BEFORE INDEX

# query = """
# SELECT overall_sentiment, COUNT(*) 
# FROM results 
# GROUP BY overall_sentiment
# """

# query1_start = time.perf_counter()

# cursor.execute(query)
# print("Query Result:", cursor.fetchall())

# query1_end = time.perf_counter()
# before_time = query1_end - query1_start

# print("Query Time (Before Index):", round(before_time, 6), "seconds")


# # ADD INDEXES

# print("Applying Indexes...")

# cursor.execute("CREATE INDEX IF NOT EXISTS idx_sentiment ON results(overall_sentiment)")
# cursor.execute("CREATE INDEX IF NOT EXISTS idx_total_score ON results(total_score)")
# cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON results(timestamp)")

# conn.commit()


# # QUERY AFTER INDEX

# query2_start = time.perf_counter()

# cursor.execute(query)
# print("Query Result:", cursor.fetchall())

# query2_end = time.perf_counter()
# after_time = query2_end - query2_start

# print("Query Time (After Index):", round(after_time, 6), "seconds")


# # PERFORMANCE COMPARISON

# if before_time == 0:
#     print("\nQuery executed too fast to measure improvement.")
# else:
#     improvement = ((before_time - after_time) / before_time) * 100
#     print("\nPerformance Improvement:", round(improvement, 2), "%")


# # CLOSE CONNECTION

# conn.close()

# overall_end = time.perf_counter()
# print("\nTotal Execution Time:", round(overall_end - overall_start, 4), "seconds")
# print("Process Completed Successfully")




# FOR 15000 RECORDS

# PS C:\Users\rlith\OneDrive\Desktop\INFOSYS SPRINGBOARD INTERN\TASK\TASK-4 (STORAGE IMPROVE)> python main.py
# Loading CSV...
# Total Records Loaded: 1597
# Processing and Inserting Data...
# Insert Time: 0.296 seconds
# Query Result: [('Negative', 129), ('Neutral', 927), ('Positive', 541)]
# Query Time (Before Index): 0.002147 seconds
# Applying Indexes...
# Query Result: [('Negative', 129), ('Neutral', 927), ('Positive', 541)]
# Query Time (After Index): 0.000673 seconds

# Performance Improvement: 68.66 %

# Total Execution Time: 0.7165 seconds
# Process Completed Successfully