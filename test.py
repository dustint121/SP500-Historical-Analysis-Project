import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import requests
import json
import main
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pandas_market_calendars as mcal
from market_cap_funct import *



load_dotenv()
apikey = os.environ.get("apikey")
tiingo_token = os.environ.get("tiingo_token")

# sp_500_stocks_1996 = pd.read_pickle("github_dataset.pkl")["date"]


def get_tiingo_meta_data_index():
    headers = {'Content-Type': 'application/json'}
    URL = "https://api.tiingo.com/tiingo/fundamentals/meta?token=" + tiingo_token
    requestResponse = requests.get(URL, headers=headers)
    meta_data_list = requestResponse.json()
    starting_indices = {}
    for index, stock in enumerate(meta_data_list):
        # if stock["name"] in [None, ""]:
        #     continue
        first_letter = stock['ticker'][0].upper()
        if first_letter not in starting_indices:
            starting_indices[first_letter] = index
    print(starting_indices)


tiingo_meta_data_index = {'A': 0, 'B': 1787, 'C': 2763, 'D': 4607, 'E': 5244, 'F': 6052, 'G': 6866, 'H': 7687, 'I': 8349, 
                          'J': 9170, 'K': 9383, 'L': 9730, 'M': 10382, 'N': 11485, 'O': 12269, 'P': 12752, 'Q': 13899, 'R': 14030,
                            'S': 14729, 'T': 16316, 'U': 17321, 'V': 17665, 'W': 18158, 'X': 18638, 'Y': 18773, 'Z': 18851}



# directory = 'company_profiles'
# sectors = {}
# industries = {}

# for filename in os.listdir(directory):
#     if filename.endswith('.json'):
#         with open(os.path.join(directory, filename), 'r') as file:
#             data = json.load(file)
#             sector = data["sector"]
#             industry = data["industry"]
#             exchange = data["exchange"]
#             if sectors.get(sector) != None : sectors[sector] += 1
#             else: sectors[sector] = 1

#             if industries.get(industry) != None : industries[industry] += 1
#             else: industries[industry] = 1

# sectors = dict(sorted(sectors.items(), key=lambda item: item[1], reverse=True))
# industries = dict(sorted(industries.items(), key=lambda item: item[1], reverse=True))

# print(sectors)
# print("\n")
# print(industries)

# print("\n")
# print(len(sectors), len(industries))

















# page_url = "https://companiesmarketcap.com/monsanto/marketcap/"
# response = requests.get(page_url)
# soup = BeautifulSoup(response.content, 'html.parser')
# # print(soup.prettify())
# data = soup.find("script",{"type": "text/javascript"}).string
# # type="text/javascript"

# # Use regex to find the data variable
# pattern = re.compile(r"data\s*=\s*(\[\{.*?\}\]);")
# match = pattern.search(data)
# if match:
#     data = match.group(1)
#     data = json.loads(data)
#     for i, point in enumerate(data):
#         unix_time, market_cap_in_millions = point["d"], point["m"]
#         market_cap_in_millions = point["m"]
#         del data[i]['m']
#         data[i]["marketcap"] = market_cap_in_millions * 1000000
#     print(data[:5], data[-5:])
# else:
#     print("Data not found")


# unix_time = int(datetime.strptime("2018-06-06" + " 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp())
# print(unix_time)

# unix_time = 971827200  # This is for illustration purposes
# # Convert Unix time to datetime object
# dt = datetime.fromtimestamp(unix_time, tz=timezone.utc)
# # Extract day, month, and year
# day = dt.day
# month = dt.month
# year = dt.year
# print(f"Month: {month}, Day: {day}, Year: {year}")
# print(dt.hour, dt.minute, dt.second)









# nyse = mcal.get_calendar('NYSE')

# date1 = datetime.strptime("2017-03-19", "%Y-%m-%d")
# date2 = datetime.strptime("2017-03-20", "%Y-%m-%d")

# print(nyse.valid_days(start_date=date1, end_date=date2))

# a = nyse.valid_days(start_date=date1, end_date=date2)

# # print(date1.__str__()[:10])

# print(nyse.valid_days(start_date=date1, end_date=date2)[0].date().__str__())

# print(a)
# a = a[1:]
# print(a)





#132 screenshots
#142 downloads
#correct # of total files: 274


# date1 = datetime.strptime("2017-03-19", "%Y-%m-%d")
# print(date1.__str__()[:10])


# data = pd.read_csv("historical_stock_price\\162_KHC.csv")
# data = pd.read_csv("historical_stock_price\\728_DELL.csv")


# print(data.head(5))
# marketcaps_dict = {89:51.8, 90:51.8, 162:47, 728:24.4}
# marketcap_ratio = marketcaps_dict[728] / 13.73

# indices =list(data[data["Date"] == "Please note that these closing prices reflect the Cumulative Split-Adjusted Price."].index)

# marketcap_data = []
# for i in range(len(indices) - 1):
#     start, stop = indices[i] + 1, indices[i+1]
#     marketcap_data_part1 = []
#     marketcap_data_part2 = []
#     for index in range(start, stop):
#         date_parts_1 = data.iloc[index]["Date"].split("/")
#         year, month, day = (date_parts_1[2], date_parts_1[0] if int(date_parts_1[0]) >= 10 else "0" + date_parts_1[0]
#                             ,date_parts_1[1] if int(date_parts_1[1]) >= 10 else "0" + date_parts_1[1])
#         date_1 = year + "-" + month + "-" + day
#         marketcap_1 = round(float(data.iloc[index]["Stock Close Price"] * marketcap_ratio * 1000000000), 2)
#         date_parts_2 = data.iloc[index]["Date.1"].split("/")
#         year, month, day = (date_parts_2[2], date_parts_2[0] if int(date_parts_2[0]) >= 10 else "0" + date_parts_2[0]
#                             ,date_parts_2[1] if int(date_parts_2[1]) >= 10 else "0" + date_parts_2[1])
#         date_2 = year + "-" + month + "-" + day
#         marketcap_2 = round(float(data.iloc[index]["Stock Close Price.1"] * marketcap_ratio * 1000000000), 2)
#         marketcap_data_part1.append({"date":date_1, "market_cap": marketcap_1})
#         marketcap_data_part2.append({"date":date_2, "market_cap": marketcap_2})

#     marketcap_data = marketcap_data + marketcap_data_part1 + marketcap_data_part2 
# for index in range(indices[-1]+1, len(data)):
#     date_parts = data.iloc[index]["Date"].split("/")
#     year, month, day = (date_parts[2], date_parts[0] if int(date_parts[0]) >= 10 else "0" + date_parts[0]
#                         ,date_parts[1] if int(date_parts[1]) >= 10 else "0" + date_parts[1])
#     date = year + "-" + month + "-" + day
#     marketcap = round(float(data.iloc[index]["Stock Close Price"] * marketcap_ratio * 1000000000), 2)
#     marketcap_data.append({"date":date, "market_cap": marketcap})



# # print(indices)

# print(marketcap_data[:5])
# print(marketcap_data[-5:])




