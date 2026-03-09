

from extract_data import *

import sys
import time
from store_data_database import *

DIR_PATH = "C:/Users/vishal.kushvanshi/PycharmProjects/data_extract_data_save/PDP/PDP/"

def main():
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    db_call_count = 0
    create_table()
    print("table and db create")
    restaurant_detail_list = []
    for raw_dict in read_files_zip(DIR_PATH, start, end):
        result = extract_data(raw_dict)
        if not result:
            continue

        restaurant_detail_list.append(result)

        if len(restaurant_detail_list) >= 1000:  # large
            db_call_count += 1
            insert_data_in_table(list_data=restaurant_detail_list)
            restaurant_detail_list.clear()

    if restaurant_detail_list:
        db_call_count += 1
        insert_data_in_table(list_data=restaurant_detail_list)
    print("extract result data ; ", type(restaurant_detail_list))
    print("total db call : ", db_call_count)
    print("add recode  database time .. : ", time.time() - start)




if __name__ == "__main__":
    start = time.time()
    main()

    end = time.time()
    print("time different  : ", end - start)





#
# import sys
# import time
# # from turtledemo.penrose import start
#
# from extract_data import *
# from store_data_database import *
#
# from restaurant_database import create_db, create_table, insert_data_in_table
# from extract_data_from_zip_file import read_files_zip, extract_grab_food_data
#
# DIR_PATH = "C:/Users/vishal.kushvanshi/PycharmProjects/restaurant_59k_pages_data_extract_save/PDP/PDP/"
#
# def main():
#     # start = int(sys.argv[1])
#     # end = int(sys.argv[2])
#
#     create_table()
#     print("table and db create")
#     restaurant_detail_list = []
#     for raw_dict in read_files_zip(DIR_PATH, start, end):
#         result = extract_grab_food_data(raw_dict)
#         if not result:
#             continue
#
#         restaurant_detail_list.append(result)
#
#         if len(restaurant_detail_list) >= 500:  # large batch
#             insert_data_in_table(list_data=restaurant_detail_list)
#             restaurant_detail_list.clear()
#
#     if restaurant_detail_list:
#             insert_data_in_table(list_data=restaurant_detail_list)
#     print("extract result data ; ", type(restaurant_detail_list))
#     print("add recode  database time .. : ", time.time() - start)
#
#
#
#
# if __name__ == "__main__":
#     start = time.time()
#     main()
#
#     end = time.time()
#     print("time different  : ", end - start)