import sqlite3

conn = sqlite3.connect("amazon_aspect_sentiment.db")  #sentiment_results.db
cursor = conn.cursor()

cursor.execute("SELECT * FROM results LIMIT 10")
print(cursor.fetchall())

conn.close()
