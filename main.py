import requests
import csv
from datetime import datetime
import json
import time
import os
import pandas as pd

def load_cookie(cookies_json) -> str:
    """Load cookie JSON to Python dict and format it to string."""
    with open(cookies_json) as f:
        cookies_data = json.load(f)

    cookies_string = ""
    for data in cookies_data:
        temp = f"{data['name']}={data['value']}"
        if cookies_string:
            cookies_string += "; "
        cookies_string += temp

    return cookies_string

def load_existing_data(file_name):
    """Load existing data to avoid duplicates."""
    if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
        df_existing = pd.read_csv(file_name)
        if 'id_review' in df_existing.columns:
            existing_ids = set(df_existing['id_review'])
        else:
            print(f"Warning: 'id_review' column not found in {file_name}. No duplicates will be removed.")
            existing_ids = set()
        return df_existing, existing_ids
    return pd.DataFrame(), set()

def shopee(url, cookies_json, max_data=10000):
    file_name = "shoope_rating_camille_new_4.csv"

    # Define consistent fieldnames
    fieldnames = ["id_review", "nama pengguna", "produk", "review", "rating", "waktu transaksi"]

    # Load existing data and check for duplicates
    existing_data, existing_ids = load_existing_data(file_name)

    shop_url = url.split("/")
    cookies = load_cookie(cookies_json)

    if not cookies:
        print("Cookies not valid")
        return

    headers = {
        "content-type": "application/json", 
        "cookie": cookies,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    user_id = shop_url[4]
    shop_id = shop_url[5].replace("rating?shop_id=", "")
    offset = 0
    limit = 100
    result = []

    while len(result) + len(existing_data) < max_data:
        try:
            api_url = f"https://shopee.co.id/api/v4/seller_operation/get_shop_ratings_new?limit={limit}&offset={offset}&replied=false&shopid={shop_id}&userid={user_id}"
            req = requests.get(api_url, headers=headers)
            
            if req.status_code != 200:
                print(f"Request failed with status code {req.status_code}")
                break

            data_req = req.json()

            if data_req.get("error") != 0:
                print(f"API error: {data_req.get('error_msg', 'Unknown error')}")
                break

            if not data_req.get("data"):
                print("No more data available")
                break

            data_review = data_req["data"].get("items", [])

            if not data_review:
                print("No reviews found")
                break

            for value in data_review:
                id_review = value.get("ctime", 0)
                if id_review in existing_ids:
                    continue  # Skip if duplicate

                # Ensure product_items is not empty
                product_items = value.get("product_items", [])
                product_name = product_items[0].get("name", "Unknown Product") if product_items else "Unknown Product"

                data_result = {
                    "id_review": id_review,
                    "nama pengguna": value.get("author_username", ""),
                    "produk": product_name,
                    "review": value.get("comment", ""),
                    "rating": value.get("rating_star", ""),
                    "waktu transaksi": datetime.fromtimestamp(value.get("ctime", 0)).strftime("%Y-%m-%d %H:%M"),
                }
                result.append(data_result)
                existing_ids.add(id_review)
                print(f"Getting review from {data_result['nama pengguna']}")

                if len(result) + len(existing_data) >= max_data:
                    break

            offset += limit
            time.sleep(2)

        except Exception as e:
            print(f"An error occurred: {e}")
            break

    # Save non-duplicate results to CSV
    if result:
        df_new = pd.DataFrame(result)
        df_combined = pd.concat([existing_data, df_new]).drop_duplicates(subset='id_review')
        df_combined.to_csv(file_name, index=False, encoding='utf-8')
        print(f"Data saved at {file_name} with {len(df_combined)} unique entries")
    else:
        print("No new data to save")

if __name__ == '__main__':
    url_shop = "https://shopee.co.id/buyer/195453712/rating?shop_id=195450779"
    cookies_json = "cookies.json"
    shopee(url_shop, cookies_json, max_data=10000)
