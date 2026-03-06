

import json
import os
import gzip


def read_files_zip(path: str, start, end):
    try:
        files = os.listdir(path)
        files.sort()
        for file in files[start:end]:
            filename = os.path.join(path, file)
            with gzip.open(filename, "rt", encoding="utf-8") as f:
                yield json.load(f)
    except Exception as e:
        print("Error in func:", read_files_zip.__name__, '\nError: ', e)


def extract_data(dict_data):
    restaurant_detail = {}
    restaurant_detail["restaurant_name"] = dict_data.get("data").get("title")
    restaurant_detail["restaurant_id"] = dict_data.get("data").get("uuid")
    restaurant_detail["image_url"] = []
    if not dict_data.get("data").get("heroImageUrls"):
        return restaurant_detail
    for data in dict_data.get("data").get("heroImageUrls"):
        restaurant_detail["image_url"].append(data.get("url"))
    location_value = dict_data.get("data").get("location")

    restaurant_detail["location"] = {
        "address" : location_value.get("address"),
        "streetAddress" : location_value.get("streetAddress"),
        "city" : location_value.get("city"),
        "country" : location_value.get("country"),
        "postalCode" : location_value.get("postalCode"),
        "region" : location_value.get("region"),
        "latitude" : location_value.get("latitude"),
        "longitude" : location_value.get("longitude"),
    }

    restaurant_detail["timeing"] = []
    timeing = dict_data.get("data").get("hours")
    for data in timeing:
        dayRange = data.get("dayRange")
        restaurant_detail["timeing"].append({
            "dayRange": dayRange,
            "sectionHours": []
        })
        if not data.get("sectionHours"):
            continue
        for time_dict  in data.get("sectionHours"):
            temp_dict = {
                "startTime"  : round(time_dict.get("startTime") / 3600, 2),
                "endTime"  : round(time_dict.get("endTime") / 3600, 2)
            }
            restaurant_detail["timeing"][len(restaurant_detail["timeing"]) - 1].get("sectionHours").append(temp_dict)
    restaurant_detail["categories"] = []
    if not dict_data.get("data").get("catalogSectionsMap"):
        return restaurant_detail
    categories_list = dict_data.get("data").get("catalogSectionsMap").get("0ad5db85-c10f-5ad6-897c-f8ef6bd5cc78")
    if not categories_list:
        return restaurant_detail
    for categories_detail in categories_list:
        restaurant_detail["categories"].append({
            "categories_id" : categories_detail.get("catalogSectionUUID"),
            "categories_name" : categories_detail.get("payload").get("standardItemsPayload").get("title").get("text"),
            "category_items" : [],
        })
        catalogItems = categories_detail.get("payload").get("standardItemsPayload").get("catalogItems")
        for items_detail in catalogItems:
            temp_dict = {
                "item_id" : items_detail.get("uuid"),
                "item_name" : items_detail.get("title"),
                "item_image_url" : items_detail.get("imageUrl"),
                "description" : items_detail.get("itemDescription"),
                "price" : items_detail.get("priceTagline").get("text")
            }
            categories_last_index = len(restaurant_detail["categories"])-1
            restaurant_detail["categories"][categories_last_index].get("category_items").append(temp_dict)
    return restaurant_detail

