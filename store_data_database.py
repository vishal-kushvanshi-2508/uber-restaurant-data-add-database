





### note--- delete your log  file .. every time-- before run again program otherwise database log file size
###            will be incrise more then 152 MB.....


from typing import List, Tuple
import json
import re
from threading import Thread

import mysql.connector # Must include .connector

from concurrent_log_handler import ConcurrentRotatingFileHandler

import logging

formatter = logging.Formatter(
    "{lineno} | {asctime} | {name} | {levelname} | {threadName} | {message}",
    style="{"
)

# -------- Logger 1 --------
logger = logging.getLogger("uber")
logger.setLevel(logging.DEBUG)

uber_handler = ConcurrentRotatingFileHandler(
    "uber_logging_file.log",
    mode="a",
    # maxBytes=50 * 1024 * 1024,
    # backupCount=5,
    encoding="utf-8"
)

uber_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(uber_handler)

# -------- Logger 2 --------
db_file = logging.getLogger("database")
db_file.setLevel(logging.DEBUG)

db_handler = ConcurrentRotatingFileHandler(
    "database_log_file.log",
    mode="a",
    # maxBytes=50 * 1024 * 1024,
    # backupCount=5,
    encoding="utf-8"
)

db_handler.setFormatter(formatter)

if not db_file.handlers:
    db_file.addHandler(db_handler)



DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "uber_restaurant_db"
}

def get_connection():
    try:
        ## here ** is unpacking DB_CONFIG dictionary.
        connection = mysql.connector.connect(**DB_CONFIG)
        ## it is protect to autocommit
        connection.autocommit = False
        logger.info("Database connection established")
        return connection
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def create_db():
    connection = get_connection()
    # connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS uber_restaurant_db;")
    connection.commit()
    connection.close()


def create_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        logger.info("Starting table creation")
        table_queries = {
            "restaurant_detail": """
                CREATE TABLE IF NOT EXISTS restaurant_detail(
                id INT AUTO_INCREMENT PRIMARY KEY,
                restaurant_id VARCHAR(300) UNIQUE,
                restaurant_name VARCHAR (300),
                image_url TEXT,
                location TEXT,
                timeing TEXT );
            """,
            "category_detail": """
                CREATE TABLE IF NOT EXISTS category_detail(
                id INT AUTO_INCREMENT PRIMARY KEY,
                restaurant_id VARCHAR(100),
                categories_id VARCHAR(150),
                categories_name VARCHAR(150),
                item_id VARCHAR(150) UNIQUE ,
                item_name VARCHAR(150) NOT NULL,
                image_url TEXT,
                description TEXT ,
                price VARCHAR(170) ,
                INDEX idx_restaurant_id (restaurant_id),
                CONSTRAINT fk_restaurant
                FOREIGN KEY (restaurant_id) REFERENCES restaurant_detail(restaurant_id)
                ON DELETE CASCADE  );
            """
        }
        for table_name, query in table_queries.items():
            query_without_enter =  " ".join(query.split())
            db_file.info(
                query_without_enter
            )
            cursor.execute(query)
        connection.commit()
        logger.info("All tables checked/created successfully")
    except Exception as e:
        logger.exception("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()



### using thread

def fun1(sql_query, batch ):
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.executemany(sql_query, batch)
        query_without_enter = " ".join(sql_query.split())
        values = ", ".join(str(t_data) for t_data in batch)
        recovery_sql_query = re.sub(
            r"\(\s*(%s\s*,\s*)*%s\s*\)",
            lambda m: values,
            query_without_enter
        )
        db_file.info(
            recovery_sql_query
        )
        connection.commit()
    except Exception as e:
        logger.error(f"Batch insert failed error={e}")

batch_size_length = 100
def data_commit_batches_wise(sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    threads = []
    logger.info(
        f"Starting batch processing total_records={len(sql_query_value)}"
    )
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        thread_obj = Thread(target=fun1, args=(sql_query, batch))
        thread_obj.start()
        threads.append(thread_obj)
        # thread_obj.join()

    for tread_obj in threads:
        tread_obj.join()
    logger.info(f"Completed batch processing threads={len(threads)}")
    return len(threads)



#
# ### without thread
# batch_size_length = 100
# def data_commit_batches_wise(sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
#     ## this is save data in database batches wise.
#     connection = get_connection()
#     cursor = connection.cursor()
#     batch_count = 0
#     for index in range(0, len(sql_query_value), batch_size):
#         batch = sql_query_value[index: index + batch_size]
#         cursor.executemany(sql_query, batch)
#         batch_count += 1
#         connection.commit()
#     return batch_count


def insert_data_in_table(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    parent_sql = """INSERT INTO restaurant_detail
                                (restaurant_id, restaurant_name, image_url, location, timeing )
                                VALUES (%s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE restaurant_id = restaurant_id;"""

    child_sql = """INSERT INTO category_detail
                                   (restaurant_id, categories_id, categories_name, item_id, item_name, image_url, description, price )
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s )
                                ON DUPLICATE KEY UPDATE
                                item_name = VALUES(item_name),
                                price = VALUES(price);"""
    try:
        rest_values = []
        menu_values = []
        for dict_data in list_data:
            rest_values.append( (
                dict_data.get("restaurant_id"),
                dict_data.get("restaurant_name"),
                json.dumps(dict_data.get("image_url")),  # convert list -> JSON
                json.dumps(dict_data.get("location")),  # convert dict -> JSON
                json.dumps(dict_data.get("timeing"))  # convert list -> JSON
            ))

            categories_list = dict_data.get("categories", [])
            if not categories_list:
                continue
            for categories_dict in categories_list:
                categories_id = categories_dict.get("categories_id")
                categories_name = categories_dict.get("categories_name")

                for item_dict in categories_dict.get("category_items", []):
                    restaurant_id = dict_data.get("restaurant_id")
                    item_id = item_dict.get("item_id")
                    item_name = item_dict.get("item_name")
                    image_url = item_dict.get("item_image_url")
                    description = item_dict.get("description")
                    price = item_dict.get("price")
                    # Check if 'a' is a tuple
                    if isinstance(categories_id, tuple):
                        categories_id, = categories_id[0]  # Extract the first element
                    if isinstance(categories_name, tuple):
                        categories_name, = categories_name[0]  # Extract the first element
                    # if isinstance(categories_id, tuple):
                    #     categories_id, = categories_id[0]  # Extract the first element
                    menu_values.append((
                        restaurant_id,
                        categories_id,
                        categories_name,
                        item_id,
                        item_name,
                        image_url,
                        description,
                        price
                    ))
        try:
            batch_count = data_commit_batches_wise(parent_sql, rest_values)
            logger.info(f"Parent batches executed count={batch_count}")
            # print("batch size  parent : ", batch_count)
            batch_count = data_commit_batches_wise(child_sql, menu_values)
            logger.info(f"Child batches executed count={batch_count}")
            # print("batch size  child : ", batch_count)
        except Exception as e:
            print(f"batch can not. Error : ")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        logger.exception("Transaction failed. Rolling back")
    except:
        print("except error raise ")
    finally:
        connection.close()





