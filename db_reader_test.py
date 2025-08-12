import pymysql

db_params = {
    "db": "derivates_crawler",
    "user": "reader",
    "password": "OuWoje3zea",
    "host": "poc-kl.cluster-cgbcqc4g9atp.ap-southeast-1.rds.amazonaws.com",
}

conn = pymysql.connect(**db_params)

query = "select * from yahoo_stock_price limit 2"
with conn.cursor() as cur:
    cur.execute(query)
    rows = cur.fetchall()

print(rows)
