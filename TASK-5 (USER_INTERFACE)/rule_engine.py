import json
import os
import re
from database import get_connection

RULES_FILE = "rules.json"


def load_rules():
   
    # Load rules from rules.json safely.
  

    if not os.path.exists(RULES_FILE):
        return {}

    try:
        with open(RULES_FILE, "r") as f:
            data = json.load(f)

        rules = {}

        for word, value in data.items():
            try:
                rules[word.lower()] = float(value)
            except:
                rules[word.lower()] = 0.0

        return rules

    except:
        return {}
    

def apply_rules():

    rules = load_rules()
    if not rules:
        return 0

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, chunk FROM chunks")
    rows = cursor.fetchall()

    batch_updates = []

    for chunk_id, text in rows:

        score = 0
        matched_words = []

        words = re.findall(r"\b\w+\b", text.lower())

        for word, val in rules.items():
            if word in text.lower():
                score += val
                matched_words.append(word)

        if score > 0:
            sentiment = "Positive"
        elif score < 0:
            sentiment = "Negative"
        else:
            sentiment = "Neutral"

        batch_updates.append(
            (score, ",".join(set(matched_words)), sentiment, chunk_id)
        )

        if len(batch_updates) >= 5000:
            cursor.executemany(
                "UPDATE chunks SET score=?, matched_rules=?, sentiment=? WHERE id=?",
                batch_updates
            )
            batch_updates = []

    if batch_updates:
        cursor.executemany(
            "UPDATE chunks SET score=?, matched_rules=?, sentiment=? WHERE id=?",
            batch_updates
        )

    conn.commit()
    conn.close()

    return len(rows)