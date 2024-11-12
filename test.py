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



load_dotenv()
apikey = os.environ.get("apikey")
tiingo_token = os.environ.get("tiingo_token")

# sp_500_stocks_1996 = pd.read_pickle("github_dataset.pkl")["date"]


# print(len(sp_500_stocks_1996))


# #need sectors for these stocks
# # ['FLIR', 'VAR', 'CXO', 'TIF', 'NBL', 'ETFC', 'AGN', 'RTN', 'WCG', 'STI', 'VIAB', 'CELG', 'TSS', 'APC', 'RHT']


# print(sp_500_stocks_1996[1142])
# print(sp_500_stocks_1996[1143])

# tickers = pd.read_pickle("github_dataset.pkl")["tickers"]

# tickers_1 = tickers[1142]
# tickers_2 = tickers[1143]

# for ticker in tickers_1:
#     if ticker not in tickers_2:
#         print(ticker)

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

# unix_time = 1528243200  # This is for illustration purposes
# # Convert Unix time to datetime object
# dt = datetime.fromtimestamp(unix_time, tz=timezone.utc)
# # Extract day, month, and year
# day = dt.day
# month = dt.month
# year = dt.year
# print(f"Month: {month}, Day: {day}, Year: {year}")
# print(dt.hour, dt.minute, dt.second)









nyse = mcal.get_calendar('NYSE')

date1 = datetime.strptime("2017-03-19", "%Y-%m-%d")
date2 = datetime.strptime("2017-03-20", "%Y-%m-%d")

print(nyse.valid_days(start_date=date1, end_date=date2))

a = nyse.valid_days(start_date=date1, end_date=date2)

# print(date1.__str__()[:10])

print(nyse.valid_days(start_date=date1, end_date=date2)[0].date().__str__())

print(a)
a = a[1:]
print(a)
