import sqlite3

DB_NAME = "text_processor.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    # CHUNKS TABLE 
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid TEXT,
        chunk TEXT,
        score REAL DEFAULT 0,
        matched_rules TEXT DEFAULT '',
        sentiment TEXT DEFAULT 'Neutral'
    )
    """)

    # INDEXES (OPTIMIZATION) 

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sentiment ON chunks(sentiment)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_score ON chunks(score)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_uid ON chunks(uid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunk ON chunks(chunk)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_matched_rules ON chunks(matched_rules)")

    conn.commit()
    conn.close()