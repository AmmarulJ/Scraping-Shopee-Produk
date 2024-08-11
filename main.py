import requests
import csv
from datetime import datetime
import json


def load_cookie(cookies_json) -> str:
    """Load cookie JSON to Python dict and format it to string."""
    with open(cookies_json) as f:
        cookies_data = json.load(f)

    cookies_string = ""
    for data in cookies_data:
        # Create a cookie string in the format "key=value"
        temp = f"{data['name']}={data['value']}"
        if cookies_string:
            cookies_string += "; "
        cookies_string += temp

    return cookies_string


def shopee(url, cookies_json):
    shop_url = url.split("/")
    cookies = load_cookie(cookies_json)

    if not cookies:
        print("Cookies not valid")
        return

    headers = {"content-type": "application/json", "cookie": cookies}

    user_id = shop_url[4]
    shop_id = shop_url[5].replace("rating?shop_id=", "")
    offset = 0
    limit = 20  # Adjust the limit as needed
    result = []

    while True:
        try:
            api_url = f"https://shopee.co.id/api/v4/seller_operation/get_shop_ratings_new?limit={limit}&offset={offset}&replied=false&shopid={shop_id}&userid={user_id}"
            req = requests.get(api_url, headers=headers)
            
            # Check if the request was successful
            if req.status_code != 200:
                print(f"Request failed with status code {req.status_code}")
                break

            data_req = req.json()

            # Check for error in response
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
                data_result = {
                    "nama pengguna": value.get("author_username", ""),
                    "produk": value.get("product_items", [{}])[0].get("name", ""),
                    "review": value.get("comment", ""),
                    "rating": value.get("rating_star", ""),
                    "waktu transaksi": datetime.fromtimestamp(value.get("ctime", 0)).strftime("%Y-%m-%d %H:%M"),
                }
                result.append(data_result)
                print(f"Getting review from {data_result['nama pengguna']}")

            offset += limit  # Increment offset to get the next page of data

        except Exception as e:
            print(f"An error occurred: {e}")
            break

    # Save to CSV with utf-8 encoding
    if result:
        keys = result[0].keys()
        with open("shoope_rating_fortklass.csv", "w", newline="", encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(result)
        print("Data saved at shoope_rating_fortklass.csv")
    else:
        print("No data to save")


if __name__ == '__main__':
    url_shop = "https://shopee.co.id/buyer/57659154/rating?shop_id=57657729"
    cookies_json = "cookies.json"
    shopee(url_shop, cookies_json)
