import requests
import json
import os
from dotenv import load_dotenv
from company_profile_func import *
from datetime import datetime
import pandas_market_calendars as mcal


load_dotenv()
apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")
username = os.environ.get("kibot_user")
password = os.environ.get("kibot_password")

#get market cap data from fmp
def get_fmp_market_cap_data(original_ticker, index):
    start_year = 2020
    end_year = 2024
    if index >  570:
        start_year = 2016
        end_year = 2020
    if index > 680:
        start_year = 2012
        end_year = 2016
    if index > 750:
        years_to_subtract = ((index - 550) // 100) * 4
        start_year = 2020 - years_to_subtract
        end_year = 2024 - years_to_subtract
    if index > 1200:
        start_year = 1922
        end_year = 1996       
    fmp_market_cap_data = []
    while True:
        fmp_url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{original_ticker}?from={start_year}-01-01&to={end_year}-12-31&apikey={apikey}"
        response = requests.get(fmp_url)
        result = response.json()
        if len(result) == 0:
            break
        fmp_market_cap_data += result
        start_year -= 5
        end_year -= 5
    fmp_market_cap_data = [{"date":data["date"],"market_cap":data["marketCap"]} for data in fmp_market_cap_data]
    fmp_market_cap_data.reverse() #reverse the list to go from earliest to latest date like tiingo data
    return fmp_market_cap_data



#get market cap data from tiingo
def get_tiingo_market_cap_data(ticker, added_date,marketcap_metadata):
    headers = {'Content-Type': 'application/json'}
    tiingo_URL = "https://api.tiingo.com/tiingo/fundamentals/" + ticker +"/daily?token=" + tiingo_token
    requestResponse = requests.get(tiingo_URL, headers=headers)
    tiingo_market_cap_data = requestResponse.json()
    tiingo_market_cap_data = [{"date":data["date"].split("T")[0],"market_cap":data["marketCap"]} 
                              for data in tiingo_market_cap_data]
    
    #get rid of possible market cap data with null market cap values at start
    for i, data in enumerate(tiingo_market_cap_data):
        if data["market_cap"] != None:
            tiingo_market_cap_data = tiingo_market_cap_data[i:]
            break

    if len(tiingo_market_cap_data) == 0:
        print("Empty tiingomarket cap data for: " + ticker)
    else: 
        #if market cap data already has enough data, return it early; else, look into stock price data
        first_date_in_market_cap_data = datetime.strptime(tiingo_market_cap_data[0]["date"], "%Y-%m-%d")
        if (first_date_in_market_cap_data <= added_date 
            or first_date_in_market_cap_data <= datetime.strptime("1998-01-02", "%Y-%m-%d")):
            return tiingo_market_cap_data

        # print("Looking at tiingo stock price data: " + ticker)
        tiingo_URL = ("https://api.tiingo.com/tiingo/daily/" + ticker + 
                    "/prices?startDate=1950-01-02&token=" + tiingo_token)
        requestResponse = requests.get(tiingo_URL, headers=headers)
        tiingo_stock_price_data = requestResponse.json()

        if (len(tiingo_stock_price_data) > len(tiingo_market_cap_data)):
            date_needed = max(added_date,  datetime.strptime("1998-01-02", "%Y-%m-%d"))
            first_date_in_market_cap_data = datetime.strptime(tiingo_market_cap_data[0]["date"], "%Y-%m-%d")

            nyse = mcal.get_calendar('NYSE')
            # Get the market open days within the specified range
            missing_trading_days = nyse.valid_days(start_date=date_needed, end_date=first_date_in_market_cap_data)
            num_missing_trading_days = len(missing_trading_days)

            marketcap_metadata["first_day_given"] = f"{first_date_in_market_cap_data.month}/{first_date_in_market_cap_data.day}/{first_date_in_market_cap_data.year}"
            marketcap_metadata["first_day_needed"] = f"{date_needed.month}/{date_needed.day}/{date_needed.year}"
            marketcap_metadata["num_trading_days_to_calculate"] = num_missing_trading_days
            if num_missing_trading_days > 250:
                print(str(first_date_in_market_cap_data) + " - " + str(date_needed))
                print("\tTiingo: needed to calculate # of additional trading days: " + str(num_missing_trading_days))

            new_market_cap_data = []
            last_stock_market_cap_ratio = None
            for daily_stock_price_data in tiingo_stock_price_data:
                current_date = datetime.strptime(daily_stock_price_data["date"].split("T")[0], "%Y-%m-%d")
                if current_date >= first_date_in_market_cap_data:
                    last_stock_market_cap_ratio = tiingo_market_cap_data[0]["market_cap"] / daily_stock_price_data["close"]
                    if current_date != first_date_in_market_cap_data:
                        print("Same date match not found")
                    break
                new_market_cap_data.append(daily_stock_price_data)
            new_market_cap_data = [{"date": data["date"].split("T")[0]
                                    ,"market_cap": round(data["close"]*last_stock_market_cap_ratio, 2)} 
                              for data in new_market_cap_data]
            tiingo_market_cap_data = new_market_cap_data + tiingo_market_cap_data

            if tiingo_stock_price_data[-1]["date"].split("T")[0] != tiingo_market_cap_data[-1]["date"]:
                print("Tiingo data error: Ending Dates aren't the same: " + ticker)

    return tiingo_market_cap_data





def kibot_market_cap():
    authenciation_request = requests.get("http://api.kibot.com/?action=login&user=" + username + "&password=" + password)
    print(authenciation_request.text)
    headers = ["Date", "Open", "High", "Low", "Close", "Volume"]
    kibot_request = requests.get("http://api.kibot.com/?action=history&symbol=MSFT&interval=daily&period=10")

    result_list = kibot_request.text.splitlines()
    stock_price_data = []

    for line in result_list:
        values = line.split(',')
        stock_price_data.append(dict(zip(headers, values)))
    print(stock_price_data)


    price_to_market_cap_ratio_dict = {}

    for data in stock_price_data:
        date_data = data["Date"].split("/")
        date = date_data[2] + "-" + date_data[0] + "-" + date_data[1]

    



#determine what image type to process(downloaded or screenshotted) by dimension(downloaded images are 4000x1600)

#for CEN, 1017, 13 days of relevant data; needed
    [2.48, 2.26, 2.11, 2.04, 2.19, 2.22, 2.18, 1.89, 1.82, 2.01, 1.97, 1.95, 2.05]

#for ITT, 1148
    [3.69, 3.71, 3.68, 3.62, 3.66, 3.37, 3.62, 3.59, 3.66, 3.66, 3.67, 3.69, 3.62, 3.67, 3.68, 3.56, 3.55, 3.67, 3.74, 3.67
     ,3.81, 3.73, 3.74, 3.79, 3.78, 3.86, 3.92, 3.86, 3.81, 3.78, 3.83, 3.83, 3.83, 3.91]

#for BBI, 1152
    [13.92, 14.18, 14.07, 14.07, 13.96, 13.43]


#store min and max of y-axis
    #will make code to double-check/adjust for screenshotted data
# finchat_marketcap_dict = {545:(15,60), }
