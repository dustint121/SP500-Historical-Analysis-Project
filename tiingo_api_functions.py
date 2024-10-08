import requests
import pandas as pd
import os
from dotenv import load_dotenv
import json

load_dotenv()
tiingo_token = os.environ.get("tiingo_token")

df = pd.read_csv("raw_sp_500_dataset_from_fmp.csv")



# tickers = df["Ticker"][:504]
tickers = df["Ticker"]
count = -1
total = 0
ticker_data_empty = []
tickers_not_found = []

ticker_data_with_less_than_2_years = []
for ticker in tickers:
    count += 1
    # print("On index: " + str(count+2))
    headers = {
    'Content-Type': 'application/json'}
    URL = "https://api.tiingo.com/tiingo/daily/" + ticker + "/prices?startDate=1950-01-02&token=" + tiingo_token
    requestResponse = requests.get(URL, headers=headers)
    data = requestResponse.json()

    if isinstance(data, dict):
        x = data["detail"]
        print("Ticker not found: " + ticker + "\tindex:" + str(count + 2))
        tickers_not_found.append({str(count + 2): ticker})
        total += 1
        continue
    elif data == [] or len(data) == 0:
        print("Have a valid result but empty list: " + ticker + "\tindex:" + str(count + 2))
        ticker_data_empty.append({str(count + 2): ticker})
        total += 1
        continue
    elif len(data) < 500:
        print("Less than 2 years of data: " + ticker + "\tindex:" + str(count + 2))
        ticker_data_with_less_than_2_years.append({str(count + 2): ticker})

    file_path = "raw_tiingo_stock_price_files/" + ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)





    # print(requestResponse.json())
    # break

print("Total missing profiles: " + str(total))

print("\n\ntickers_not_found:")
print(tickers_not_found)

print("\n\nticker_data_empty:")
print(ticker_data_empty)

print("\n\nticker_data_with_less_than_2_years:")
print(ticker_data_with_less_than_2_years)



