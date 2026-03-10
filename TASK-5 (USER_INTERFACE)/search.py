import pandas as pd
import re
from database import get_connection


# KEYWORD SEARCH
def search_keyword(keyword, limit):

    conn = get_connection()

    query = """
    SELECT *
    FROM chunks
    WHERE LOWER(chunk) LIKE LOWER(?)
    LIMIT ?
    """

    df = pd.read_sql_query(
        query,
        conn,
        params=[f"%{keyword}%", limit]
    )

    conn.close()

    return df


# REGEX SEARCH
def search_regex(pattern, limit):

    conn = get_connection()

    query = """
    SELECT *
    FROM chunks
    LIMIT 50000
    """

    df = pd.read_sql_query(query, conn)

    conn.close()

    # apply regex filtering
    result = df[df["chunk"].str.contains(pattern, flags=re.IGNORECASE, regex=True)]

    return result.head(limit)