


from typing import List, Tuple
import json


import mysql.connector # Must include .connector


DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "uber_restaurant_db"
}

def get_connection():
    ## here ** is unpacking DB_CONFIG dictionary.
    connection = mysql.connector.connect(**DB_CONFIG)
    ## it is protect to autocommit
    connection.autocommit = False
    return connection

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

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS restaurant_detail( 
            id INT AUTO_INCREMENT PRIMARY KEY, 
            restaurant_id VARCHAR(300) UNIQUE, 
            restaurant_name VARCHAR (300), 
            image_url TEXT, 
            location TEXT, 
            timeing TEXT );
            """)
    cursor.execute("""
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
            """)
    connection.commit()
    connection.close()

batch_size_length = 1000
def data_commit_batches_wise(connection, cursor, sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    batch_count = 0
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        cursor.executemany(sql_query, batch)
        batch_count += 1
        connection.commit()
    return batch_count


def insert_data_in_table(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    parent_sql = """INSERT INTO restaurant_detail
                                (restaurant_id, restaurant_name, image_url, location, timeing )
                                VALUES (%s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE restaurant_id = restaurant_id"""

    child_sql = """INSERT INTO category_detail
                                   (restaurant_id, categories_id, categories_name, item_id, item_name, image_url, description, price )
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s )
                                ON DUPLICATE KEY UPDATE 
                                item_name = VALUES(item_name),
                                price = VALUES(price)"""
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
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, rest_values)
            print("batch size  parent : ", batch_count)
            batch_count = data_commit_batches_wise(connection, cursor, child_sql, menu_values)
            print("batch size  child : ", batch_count)
        except Exception as e:
            print(f"batch can not. Error: {e}")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        print(f"Transaction failed, rolled back. Error: {e}")
    except:
        print("not show error ")
    finally:
        connection.close()
