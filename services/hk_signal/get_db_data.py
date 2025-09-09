import os
from mysql.connector import pooling
from dotenv import load_dotenv


load_dotenv()

dbconfig = {
    "host": os.getenv("KL_DB_HOST"),
    "user": os.getenv("KL_DB_USER"),
    "password": os.getenv("KL_DB_PASS"),
    "database": os.getenv("KL_DB"),
    "port": int(os.getenv("KL_DB_PORT", 3306)),
}

pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=10, **dbconfig)


def get_stock_data_from_db(code: str, end_date: str, limit: int = 200):
    query = f"""
        SELECT tradeday,
            COALESCE(adj_open, adj_close) as open_price,
            COALESCE(adj_high, adj_close) as high_price,
            COALESCE(adj_low, adj_close) as low_price,
            adj_close as close_price,
            adj_volume as volume
        FROM signal_hkex_price
        WHERE code = %s
        AND tradeday <= %s
        ORDER BY tradeday DESC
        LIMIT {int(limit)}
    """


    conn = pool.get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(query, (code, end_date))
        rows = cursor.fetchall()

        stock_records = [
            {
                "date": row["tradeday"].strftime("%Y-%m-%d"),
                "time": row["tradeday"].strftime("%H:%M:%S")
                if row["tradeday"]
                else "00:00:00",
                "open": float(row["open_price"])
                if row["open_price"] is not None
                else 0.0,
                "high": float(row["high_price"])
                if row["high_price"] is not None
                else 0.0,
                "low": float(row["low_price"]) if row["low_price"] is not None else 0.0,
                "close": float(row["close_price"])
                if row["close_price"] is not None
                else 0.0,
                "volume": int(row["volume"]) if row["volume"] is not None else 0,
            }
            for row in reversed(rows)
        ]

        print(f"Retrieved {len(stock_records)} records for stock {code}")
        return stock_records

    except Exception as e:
        print(f"Error fetching stock data for {code}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
