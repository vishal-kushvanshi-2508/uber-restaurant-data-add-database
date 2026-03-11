
import mysql.connector
import time
s = time.time()
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "actowiz",
    port = "3306",
    database = "uber_restaurant_db"
)

cursor = conn.cursor()

with open("database_log_file.log", "r", encoding="utf-8") as f:
    count_val = 0

    for line in f:
        try:
            parts = line.split("|", 5)

            if len(parts) < 6:
                continue

            sql_query = parts[5].strip()

            sql_query = sql_query.replace("None", "NULL")

            cursor.execute(sql_query)
            conn.commit()

            count_val += 1

        except Exception as e:
            print("Failed query:")
            print(count_val)
            print("Error:", e)

print("Done process:", count_val)

cursor.close()
conn.close()


print("time differ : ", time.time() -s)