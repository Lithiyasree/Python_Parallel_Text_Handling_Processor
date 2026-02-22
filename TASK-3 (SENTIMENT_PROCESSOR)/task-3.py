import csv
import sqlite3
from datetime import datetime

# ASPECT KEYWORDS

quality_keywords = {
    "good quality": 2,
    "excellent": 2,
    "durable": 2,
    "cheap quality": -2,
    "poor quality": -2
}

price_keywords = {
    "worth": 2,
    "affordable": 2,
    "expensive": -1,
    "overpriced": -2
}

delivery_keywords = {
    "fast delivery": 2,
    "on time": 2,
    "late": -1,
    "delayed": -2
}

packaging_keywords = {
    "well packed": 2,
    "damaged": -2,
    "broken": -2
}

performance_keywords = {
    "works perfectly": 2,
    "smooth": 2,
    "slow": -1,
    "not working": -2
}

# LOAD CSV
try:
    with open("amazon-product-reviews.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        reviews = list(reader)
except FileNotFoundError:
    print("CSV file not found.")
    exit()

# DATABASE
conn = sqlite3.connect("amazon_aspect_sentiment.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT,
    quality_status TEXT,
    price_status TEXT,
    delivery_status TEXT,
    packaging_status TEXT,
    performance_status TEXT,
    total_score REAL,
    overall_sentiment TEXT,
    timestamp TEXT
)
""")

for row in reviews[:500]:

    text = row["reviews.text"].lower()

    quality_score = 0
    price_score = 0
    delivery_score = 0
    packaging_score = 0
    performance_score = 0

    for word, value in quality_keywords.items():
        quality_score += text.count(word) * value

    for word, value in price_keywords.items():
        price_score += text.count(word) * value

    for word, value in delivery_keywords.items():
        delivery_score += text.count(word) * value

    for word, value in packaging_keywords.items():
        packaging_score += text.count(word) * value

    for word, value in performance_keywords.items():
        performance_score += text.count(word) * value

    quality_status = "Yes" if quality_score > 0 else "No"
    price_status = "Yes" if price_score > 0 else "No"
    delivery_status = "Yes" if delivery_score > 0 else "No"
    packaging_status = "Yes" if packaging_score > 0 else "No"
    performance_status = "Yes" if performance_score > 0 else "No"

    total_score = quality_score + price_score + delivery_score + packaging_score + performance_score

    if total_score > 0:
        overall_sentiment = "Positive"
    elif total_score < 0:
        overall_sentiment = "Negative"
    else:
        overall_sentiment = "Neutral"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
    INSERT INTO results 
    (text, quality_status, price_status, delivery_status, packaging_status, performance_status, total_score, overall_sentiment, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (text, quality_status, price_status, delivery_status, packaging_status, performance_status, total_score, overall_sentiment, timestamp))

conn.commit()
conn.close()

print("Amazon Aspect Sentiment Processing Completed")