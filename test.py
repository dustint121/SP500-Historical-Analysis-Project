import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import requests
import json
import main

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


# headers = {'Content-Type': 'application/json'}
# URL = "https://api.tiingo.com/tiingo/fundamentals/meta?token=" + tiingo_token
# requestResponse = requests.get(URL, headers=headers)
# meta_data_list = requestResponse.json()

# with open("tiingo_meta_data.json", 'w') as file:
#     json.dump(meta_data_list, file, indent=4)


#     headers = {'Content-Type': 'application/json'}
#     tiingo_URL = "https://api.tiingo.com/tiingo/daily/" + ticker + "?token=" + tiingo_token
#     tiingo_data= requests.get(tiingo_URL, headers=headers).json()
#     # print(tiingo_data)
#     if tiingo_data != {'detail': 'Not found.'}:
#         is_valid_exchange = tiingo_data["exchangeCode"] in ["NASDAQ", "NYSE"]
#         if is_valid_exchange == False and ticker in ["SBNY"]:   #later delisted from NYSE or NASDAQ
#             is_valid_exchange = True
#         company_name = company_name.lower().replace(',', '').replace('. ', ' ').replace('.', ' ').replace("'",'`')
#         is_right_company = company_name[:5] in tiingo_data["name"].lower().replace('. ', ' ').replace('.', ' ')
#         ticker_exception_list = ["DAY","WAB","CPAY","CBOE","ORLY","BXP","EL","LH","MTB","YUM","BK","GE","IBM","SLB"
#                                  ,"VFC","ZION","WU","DINO","ETFC","MAC","SIVBQ","GAP","FRCB","MRKT"
#                                  ,"GT","GGP","DPS","TE","ADT","GMCR","ATI"]
#         #need to test LOW
#         #CBOE; moved to different exchange later
#         #BXP,GE name changes
#         #MTB spelling issue wiht '&'
#         if ticker in ticker_exception_list or (is_valid_exchange and is_right_company):
#             start_date = tiingo_data["startDate"]
#             end_date = tiingo_data["endDate"]
#             if start_date == None:
#                 print("Start date is unlisted for: " + original_ticker)
#             # if have_data_from_fmp == False:
#             company_profile["company_name"] = tiingo_data["name"]
#             company_profile["is_delisted"] = "delisted" in tiingo_data["description"][:15].lower()
#             company_profile["description"] = tiingo_data["description"].replace("DELISTED - ", '')
#             # print(company_profile)
#             tiingo_meta_data_index = add_company_meta_data(ticker, company_profile, meta_data_list)
#             company_profile["tiingo_meta_data_index"] = tiingo_meta_data_index

#             file_path = "tiingo_company_data_files/" + str(i) + "_" + original_ticker + ".json"
#             os.makedirs(os.path.dirname(file_path), exist_ok=True)
#             with open(file_path, 'w') as file:
#                 json.dump(company_profile, file, indent=4)


#             # print(company_profile)
#         else:
#             if ticker not in []:
#                 print("Invalid Tiingo data for ticker symbol: " + ticker)
#             no_tiingo_data_list.append(ticker)

#     else:
#         print("No Tiingo data retrieved for: " + ticker)
#         no_tiingo_data_list.append(ticker)

#     #DPS company data is not found on meta for tiingo or on FMP


# print("No fmp data from: " + str(no_fmp_data_list))
# print("No tiingo data from: " + str(no_tiingo_data_list))




# a = main.get_market_cap_data("CNW", "CNW", 1182)
# a = main.get_market_cap_data("AAPL", "AAPL", 0)
# print(len(a))
# print(a[0])
# print(a[-1])