import sqlite3

# Connect (this creates a database file if it doesn't exist)
conn = sqlite3.connect("textdb.db")
cursor = conn.cursor()

'''
# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS result_1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text_data TEXT,
    score INTEGER
)
""")

conn.commit()
print("Database and table created successfully!")

cursor.close()
conn.close()

'''


'''
# Insert Data
text = "This system is good but has error"
score = 1

cursor.execute("INSERT INTO result_1 (text_data, score) VALUES (?, ?)", (text, score))

conn.commit()
print("Data inserted!")

cursor.close()
conn.close()

'''


# View Data
cursor.execute("SELECT * FROM result_1")
rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()
