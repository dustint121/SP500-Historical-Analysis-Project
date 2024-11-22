import requests
import json
import os
from dotenv import load_dotenv
from company_profile_func import *
from datetime import datetime
import pandas_market_calendars as mcal
import pandas as pd
import cv2 
import re
from bs4 import BeautifulSoup

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

            # marketcap_metadata["first_day_given"] = f"{first_date_in_market_cap_data.month}/{first_date_in_market_cap_data.day}/{first_date_in_market_cap_data.year}"
            # marketcap_metadata["first_day_needed"] = f"{date_needed.month}/{date_needed.day}/{date_needed.year}"
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

#4 cases
def get_misc_market_cap_data(index, ticker, marketcap_metadata):
    data = pd.read_csv("historical_stock_price\\" + str(index) + "_" + ticker + ".csv")
    marketcaps_dict = {89:51.8, 90:51.8, 162:47, 728:24.4}
    if ticker != "DELL":
        marketcap_metadata["source"] = "investing.com"
        marketcap_ratio = marketcaps_dict[index] / data.iloc[0]["Price"]
        data["Date"] = data["Date"].apply(lambda x : x.split("/")[2] + "-" + x.split("/")[0] + "-" + x.split("/")[1])
        data["Marketcap"] = data["Price"].apply(lambda x : round(x * marketcap_ratio * 1000000000, 2))
        
        marketcap_data = []
        for i in range(len(data)):
            marketcap_data.append({"date": data.iloc[i]["Date"], "market_cap": data.iloc[i]["Marketcap"]})
        marketcap_data.reverse()
        return marketcap_data
    else: #for 728, DELL
        marketcap_metadata["source"] = "https://i.dell.com/sites/csdocuments/Corporate_secure_Documents/en/dell-closing-costs.pdf"
        marketcap_ratio = marketcaps_dict[728] / 13.73

        indices =list(data[data["Date"] == "Please note that these closing prices reflect the Cumulative Split-Adjusted Price."].index)
        marketcap_data = []
        for i in range(len(indices) - 1): #4 columns of data to process, Date, Price, Date2, Price 2
            start, stop = indices[i] + 1, indices[i+1]
            marketcap_data_part1 = []
            marketcap_data_part2 = []
            for index in range(start, stop):
                date_parts_1 = data.iloc[index]["Date"].split("/")
                year, month, day = (date_parts_1[2], date_parts_1[0] if int(date_parts_1[0]) >= 10 else "0" + date_parts_1[0]
                                    ,date_parts_1[1] if int(date_parts_1[1]) >= 10 else "0" + date_parts_1[1])
                date_1 = year + "-" + month + "-" + day
                marketcap_1 = round(float(data.iloc[index]["Stock Close Price"] * marketcap_ratio * 1000000000), 2)
                date_parts_2 = data.iloc[index]["Date.1"].split("/")
                year, month, day = (date_parts_2[2], date_parts_2[0] if int(date_parts_2[0]) >= 10 else "0" + date_parts_2[0]
                                    ,date_parts_2[1] if int(date_parts_2[1]) >= 10 else "0" + date_parts_2[1])
                date_2 = year + "-" + month + "-" + day
                marketcap_2 = round(float(data.iloc[index]["Stock Close Price.1"] * marketcap_ratio * 1000000000), 2)
                marketcap_data_part1.append({"date":date_1, "market_cap": marketcap_1})
                marketcap_data_part2.append({"date":date_2, "market_cap": marketcap_2})
            print(len(marketcap_data_part1), len(marketcap_data_part2))
            marketcap_data = marketcap_data + marketcap_data_part1 + marketcap_data_part2 

        for index in range(indices[-1]+1, len(data)): #for last part of data, only 2 columns
            date_parts = data.iloc[index]["Date"].split("/")
            year, month, day = (date_parts[2], date_parts[0] if int(date_parts[0]) >= 10 else "0" + date_parts[0]
                                ,date_parts[1] if int(date_parts[1]) >= 10 else "0" + date_parts[1])
            date = year + "-" + month + "-" + day
            marketcap = round(float(data.iloc[index]["Stock Close Price"] * marketcap_ratio * 1000000000), 2)
            marketcap_data.append({"date":date, "market_cap": marketcap})
        return marketcap_data



#about 34 uses
def get_companiesmarketcap_market_cap_data(index, start_date, end_date, marketcap_metadata):
    #modify start date as needed; get later of current start date and start of 1998
    start_date_check = max(datetime.strptime(start_date, "%B %d, %Y"),  datetime.strptime("1998-01-02", "%Y-%m-%d")).__str__()[:10]
    if start_date_check == "1998-01-02": start_date = "January 2, 1998"

    URL_dict = {366:"https://companiesmarketcap.com/paramount/marketcap/"
            ,618:"https://companiesmarketcap.com/monsanto/marketcap/", 693:"https://companiesmarketcap.com/noble-corp/marketcap/"
            ,726:"https://companiesmarketcap.com/jcpenney/marketcap/", 732:"https://companiesmarketcap.com/sprint-corporation/marketcap/"
            ,766:"https://companiesmarketcap.com/national-semiconductor/marketcap/", 772:"https://companiesmarketcap.com/qwest-communications-international/marketcap/"
            ,792:"https://companiesmarketcap.com/sun-microsystems/marketcap/", 799:"https://companiesmarketcap.com/schering-plough/marketcap/"
            ,800:"https://companiesmarketcap.com/wyeth/marketcap/", 812:"https://companiesmarketcap.com/noble-corp/marketcap/"
            ,839:"https://companiesmarketcap.com/lehman-brothers/marketcap/", 852:"https://companiesmarketcap.com/bear-stearns/marketcap/"
            ,869:"https://companiesmarketcap.com/ncr-corporation/marketcap/", 871:"https://companiesmarketcap.com/first-data-corporation/marketcap/"
            ,893:"https://companiesmarketcap.com/bellsouth/marketcap/", 894:"https://companiesmarketcap.com/par-technology/marketcap/"
            ,897:"https://companiesmarketcap.com/lucent-technologies/marketcap/"
            ,907:"https://companiesmarketcap.com/gateway-inc/marketcap/", 929:"https://companiesmarketcap.com/att/marketcap/"
            ,935:"https://companiesmarketcap.com/nextel-communications/marketcap/", 940:"https://companiesmarketcap.com/veritas-technologies/marketcap/"
            ,964:"https://companiesmarketcap.com/pharmacia/marketcap/", 966:"https://companiesmarketcap.com/healthsouth/marketcap/"
            ,975:"https://companiesmarketcap.com/nortel-networks/marketcap/", 985:"https://companiesmarketcap.com/worldcom/marketcap/"
            ,987:"https://companiesmarketcap.com/compaq-computer/marketcap/", 996:"https://companiesmarketcap.com/enron/marketcap/"
            ,998:"https://companiesmarketcap.com/global-crossing/marketcap/"
            ,1027:"https://companiesmarketcap.com/seagram/marketcap/", 1056:"https://companiesmarketcap.com/warner-lambert/marketcap/"
            ,1058:"https://companiesmarketcap.com/mediaone-group/marketcap/", 1068:"https://companiesmarketcap.com/pharmacia-upjohn/marketcap/"
            ,1122:"https://companiesmarketcap.com/chrysler-corporation/marketcap/"
            }

    page_url = URL_dict[index]
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    data = soup.find("script",{"type": "text/javascript"}).string

    # Use regex to find the data variable
    pattern = re.compile(r"data\s*=\s*(\[\{.*?\}\]);")
    match = pattern.search(data)
    if match:
        data = match.group(1)
        data = json.loads(data)
        for i, point in enumerate(data):
            market_cap_in_millions = point["m"]
            del data[i]['m']
            data[i]["market_cap"] = market_cap_in_millions * (10 ** 5) #should be 10 ** 6, but data is off by 10
        # print(data[:5], data[-5:])

        #get relevant market cap range to return data
        print(start_date, end_date)
        nyse = mcal.get_calendar('NYSE') # Create a calendar for the New York Stock Exchange
        market_open_days = nyse.valid_days(start_date=start_date, end_date=end_date) # Get the market open days within the specified range
        market_cap_data = []
        for i, date in enumerate(market_open_days):
            month, day, year = date.month, date.day, date.year
            if date.month < 10: month = "0" + str(month)
            if date.day < 10: day = "0" + str(day)
            date = str(year) + "-" + str(month) + "-" + str(day)
            current_unix_time = int(datetime.strptime(date + " 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp())

            first_data = data[0]
            second_data = data[1]
            while True: #get correct two date ranges from available data
                if len(data) == 2: break
                if current_unix_time < first_data["d"] and current_unix_time < second_data["d"]: break

                if (first_data["d"] < current_unix_time < second_data["d"]) == False:
                    data.pop(0)
                    first_data = data[0]
                    second_data = data[1]
                else:
                    break
            
            #less than available date range
            if current_unix_time < first_data["d"] and current_unix_time < second_data["d"]:
                market_cap_data.append({"date":date, "market_cap":round(first_data["market_cap"], 2)})
            #between chosen date ranges
            elif (first_data["d"] < current_unix_time < second_data["d"]):
                #value between 0-1 that tells where you are between the two chosen dates
                x = (current_unix_time - first_data["d"]) / (second_data["d"] - first_data["d"])
                market_cap_range = second_data["market_cap"] - first_data["market_cap"]
                market_cap = round((market_cap_range * x) + first_data["market_cap"], 2) 
                market_cap_data.append({"date":date, "market_cap":market_cap})
            #larger than avaiable date range
            elif current_unix_time > first_data["d"] and current_unix_time > second_data["d"]:
                market_cap_data.append({"date":date, "market_cap":round(second_data["market_cap"], 2)})
            else:
                print("Unknown case")

        marketcap_metadata["source"] = "companiesmarketcap.com"
        return market_cap_data
    else:
        print("Data not found: companiesmarketcap.com")




#about 25 uses
def get_kibot_market_cap_data(index, ticker, start_date, end_date, marketcap_metadata):
    marketcap_metadata["source"] = "kibot"

    if index == 855: ticker = "HET" #edge case; changed ticker symbol later; HET -> CZR (Caesar's Entertainment)

    #need to fix start and end date to right format: year-month-day (xxxx-xx-xx)
    start_date = datetime.strptime(start_date, "%B %d, %Y").__str__()[:10]
    end_date = datetime.strptime(end_date, "%B %d, %Y").__str__()[:10]

    authenciation_request = requests.get("http://api.kibot.com/?action=login&user=" + username + "&password=" + password)
    # print(authenciation_request.text)
    headers = ["Date", "Open", "High", "Low", "Close", "Volume"]
    kibot_request = requests.get("http://api.kibot.com/?action=history&symbol=MSFT&interval=daily&period=10")
    kibot_request = requests.get("http://api.kibot.com/?action=history&symbol=" + str(ticker) + "&interval=daily"
                                 + "&startdate=" + str(start_date) + "&enddate=" + str(end_date))

    result_list = kibot_request.text.splitlines()
    stock_price_data = []
    for line in result_list:
        values = line.split(',')
        stock_price_data.append(dict(zip(headers, values)))
    # print(stock_price_data)

    ending_market_cap_dict = {620:6.11, 646:9, 648:7.8, 656:4, 657:1.6, 658:13.06, 664:14.5, 683:16.34
                            , 711:3.65, 725:6.93, 735:23.41, 743:12.05, 788:3.2
                            , 804:0.294, 809:1.05, 853:0.69, 854:7.03, 855:17.4, 857:1.82
                            , 926:0.8, 967:0.23
                            , 1041:5.69, 1071:0.63
                            , 1149:2.52}
    marketcap_price_ratio = ending_market_cap_dict[index] / float(stock_price_data[-1]["Close"])

    market_cap_data = []
    for data in stock_price_data:
        date_data = data["Date"].split("/")
        date = date_data[2] + "-" + date_data[0] + "-" + date_data[1]
        market_cap = round(float(data["Close"]) * marketcap_price_ratio * (10 ** 9), 2)
        market_cap_data.append({"date":date, "market_cap":market_cap})
    return market_cap_data

    






def get_finchat_market_cap_data(index, ticker, start_date, end_date, marketcap_metadata):
    #modify start date as needed; get later of current start date and start of 1998
    start_date_check = max(datetime.strptime(start_date, "%B %d, %Y"),  datetime.strptime("1998-01-02", "%Y-%m-%d")).__str__()[:10]
    if start_date_check == "1998-01-02": start_date = "January 2, 1998"

    #edge cases
    if index == 956: start_date = "March 13, 2001" #limited data
    if index == 989: start_date = "March 18, 1999" #limited data
    if index == 1084: end_date = "November 17, 1999" #more data than needed
    if index == 1086: end_date = "December 2, 1999" #more data than needed
    if index in [1015, 1148, 1152]: #these will be quickly processed instead of normal function
        #for CEN, 1015, 13 days of relevant data; needed
        market_caps = None
        if index == 1015:
            start_date = "March 14, 2001"
            market_caps = [2.48, 2.26, 2.11, 2.04, 2.19, 2.22, 2.18, 1.89, 1.82, 2.01, 1.97, 1.95, 2.05]
        #for ITT, 1148
        if index == 1148:
            market_caps = [3.69, 3.71, 3.68, 3.62, 3.66, 3.37, 3.62, 3.59, 3.66, 3.66, 3.67, 3.69, 3.62, 3.67, 3.68, 3.56, 3.55, 3.67, 3.74, 3.67
            ,3.81, 3.73, 3.74, 3.79, 3.78, 3.86, 3.92, 3.86, 3.81, 3.78, 3.83, 3.83, 3.83, 3.91]
        #for BBI, 1152
        if index == 1152:
            market_caps = [13.92, 14.18, 14.07, 14.07, 13.96, 13.43]
        nyse = mcal.get_calendar('NYSE') # Create a calendar for the New York Stock Exchange
        market_open_days = nyse.valid_days(start_date=start_date, end_date=end_date) # Get the market open days within the specified range
        market_cap_data = []
        for i, date in enumerate(market_open_days):
            month, day, year = date.month, date.day, date.year
            if day < 10: day = "0" + str(day)
            if month < 10: month = "0" + str(month)
            date = str(year) + "-" + str(month) + "-" + str(day)
            market_cap_data.append({"date":date, "market_cap":round(market_caps[i] * (10 ** 9), 2)})
        marketcap_metadata["image_type"] = "screenshot"
        marketcap_metadata["source"] = "finchat.io"
        return market_cap_data

    image_file = "finchat_market_cap_images/" + str(index) + "_" + ticker + ".png"
    image = cv2.imread(image_file)
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    length, height = gray_image.shape[1], gray_image.shape[0]
    min_market_cap, max_market_cap =  get_finchat_market_cap_range(index)

    
    reference_list = [] #will be a list representing the image scaled to the proper x-range(date) and y-range(market-caps)
    #process the image data to get pixel data scaled properly(x-coord scales to unix time and y-coord scales to market_caps)
    if (length == 4000 and height == 1600) or index in [784]:
        marketcap_metadata["image_type"] = "download"
        gray_image = gray_image[130:-209, 20:-50] #basic cropping of image
        length, height = gray_image.shape[1], gray_image.shape[0] #get new dimensions
        reference_list = process_finchat_image_download(gray_image, length, height, start_date, end_date, min_market_cap, max_market_cap)
    else:
        marketcap_metadata["image_type"] = "screenshot"
        reference_list = process_finchat_image_screenshot(gray_image, length, height, start_date, end_date, min_market_cap, max_market_cap)


#NOTE: might currently not working for downloaded images
    #seems to be for "Line ened too early": definitely not working for 1000

    nyse = mcal.get_calendar('NYSE') # Create a calendar for the New York Stock Exchange
    market_open_days = nyse.valid_days(start_date=start_date, end_date=end_date) # Get the market open days within the specified range

    market_cap_data = []
    stock_price_list = reference_list.copy()
    for day in market_open_days:
        month, day, year = day.month, day.day, day.year
        if day < 10: day = "0" + str(day)
        if month < 10: month = "0" + str(month)
        date = str(year) + "-" + str(month) + "-" + str(day)
        unix_time = int(datetime.strptime(date + " 16:00:00", "%Y-%m-%d %H:%M:%S").timestamp())

        market_cap = None
        while(True):
            if len(stock_price_list) == 0:
                break
            if len(stock_price_list) == 1: #occurs after last point of list
                if abs(unix_time - stock_price_list[0][0]) <= (86400 * 7): #at most a week of time difference
                    market_cap = round(stock_price_list[0][1] * 1000000000, 2)
                    market_cap_data.append({"date":date, "market_cap":market_cap})
                    break
                else:
                    print("Last date is more than a day apart: " + date)

            if stock_price_list[0][0] <= unix_time <= stock_price_list[1][0]:
                ratio = (stock_price_list[1][1] - stock_price_list[0][1]) / (stock_price_list[1][0] - stock_price_list[0][0])
                market_cap = (unix_time - stock_price_list[0][0]) * ratio + stock_price_list[0][1]
                market_cap = round(market_cap * 1000000000, 2)
                market_cap_data.append({"date":date, "market_cap":market_cap})
                break
            else:
                stock_price_list.pop(0)


    marketcap_metadata["source"] = "finchat.io"
    return market_cap_data













def process_finchat_image_download(gray_image, length, height, start_date, end_date, min_market_cap, max_market_cap):
    color_frequency = {} #gets the count of each color's pixel count in the image
    color_occurences = {} #records the first occurence of a color at each 'x' coordinate {x_0:{color1:y_0, color2:y_1}, x_1...}
    for x in range(length):
        if color_occurences.get(x) is None: color_occurences[x] = {}
        for y in range(height):
            color_value = gray_image[y, x]
            if color_frequency.get(color_value) is None: color_frequency[color_value] = 0
            else: color_frequency[color_value] += 1

            if color_occurences[x].get(color_value) is None: color_occurences[x][color_value] = y

    #chart should have only 3 major values: black(blackground), grid color(), line color; 
        # will remove the first two from dictionary; afterward, the line color should have the largest # of occurences
    last_y = height - 1
    #will look at first part of the bottom y-length; should only be the black background and the grid lines
        #Note: this assumes that the stock line will never touch the bottom
    # for x in range(50):
    #     color = gray_image[last_y, x]
    #     if color not in [0,45]: #these are the possible background colors
    #         if color_frequency.get(0) != None: del color_frequency[0]
    #         if color_frequency.get(45) != None: del color_frequency[45]
    #         del color_frequency[color]
    #         break
    # if color_frequency.get(0) is not None: print("Error; Black is not removed")
    # color_frequency = dict(sorted(color_frequency.items(), key=lambda item: item[1], reverse=True))
    # line_color = int(list(color_frequency.keys())[0]) #the color for the stock line
    line_color = 105
    # print("Line color: " + str(line_color))
    # print(color_frequency.get(105))
    x_start, x_end = None, None #get the range: start and ending x-coord locations of the stock line 
    for x_coord in color_occurences:
        if line_color in color_occurences[x_coord] and x_start == None:
            x_start = x_coord
        #after finding start of line, find end of line
        if x_start != None and line_color not in color_occurences[x_coord]:
            x_end = x_coord - 1
            if length - x_end >= 100: #check if line detection ends too early
                print("Line ended too early?")
                return []
            break

    #get pixel coordinates(x,y) of the stock line
    pixel_coordinates = []
    for i in range(x_start,x_end):
        x_coord = i - x_start
        if color_occurences[i].get(line_color) is None:
            print("error at: " + str(i))
            break
        y_coord = height - color_occurences[i].get(line_color) #need to reverse coordinates
        pixel_coordinates.append([x_coord,y_coord]) 
   
    #scale the pixel_coordinates properly to dates(x-asix) and the marketcap range(y-axis)
    reference_list = []
    added_date = start_date
    removed_date = end_date
    unix_added_date = int(datetime.strptime(added_date + " 16:00:00", "%B %d, %Y %H:%M:%S").timestamp())
    unix_removed_date = int(datetime.strptime(removed_date + " 16:00:00", "%B %d, %Y %H:%M:%S").timestamp())
    x_slope = (unix_removed_date - unix_added_date) / (x_end - x_start) #new axis
    y_slope = (max_market_cap - min_market_cap) / height
    for i in range(len(pixel_coordinates)):
        temp_x_coord = pixel_coordinates[i][0]
        x_coord = x_slope * temp_x_coord + unix_added_date
        temp_y_coord = pixel_coordinates[i][1]
        y_coord = (y_slope * temp_y_coord) + min_market_cap
        reference_list.append([x_coord,y_coord])

    return reference_list



def process_finchat_image_screenshot(gray_image, length, height, start_date, end_date, min_market_cap, max_market_cap):
    #get boundaries for graph, excluding pixels containing the x-axis and y-axis
    lower_boundary, left_boundary, right_boundary, top_boundary = None, None, None, None

    white_frequency = {} #gets the count of each color's pixel count in the image
    white_occurences = {} #records the first occurence of a color at each 'x' coordinate {x_0:{color1:y_0, color2:y_1}, x_1...}
    for y in range(round(height*0.6),height):
        for x in range(length):
            color_value = gray_image[y, x]
            if color_value == 255:
                if white_frequency.get(y) is None:
                    white_frequency[y] = 0
                    white_occurences[y] = [x]
                else:
                    white_frequency[y] += 1
                    white_occurences[y].append(x)
        if white_frequency.get(y) is not None and white_frequency[y]/length > 0.75: #found the y-axis here; is >=75% white pixels
            break

    y_values = list(white_occurences.keys())
    #the last y_value should be the top layer/y-value of the x-axis
    #the second-to-last y_value shoud have values of the x-axis where the axis intersect
        #also the lower_boundary, excluding the x-axis
    lower_boundary = y_values[-2] #the pixels above the x-axis; y-value
    left_boundary = white_occurences[y_values[-2]][-1] + 1 #the pixels after the y-axis; x-value
    for y in range(lower_boundary, -1, -1):
        color = gray_image[y][left_boundary - 1]
        if color != 255:
            top_boundary = y + 1
            break
    for x in reversed(white_occurences[lower_boundary + 1]): #look at pixels on line directly above x-axis in reverse
        color = gray_image[lower_boundary][x]
        if color != 45:
            right_boundary = x
            break
    
    #perform basic checks for boundary locations
    if (left_boundary < (0.1*length)) == False: print("Possible issue with left boundary: " + str(left_boundary))
    if (right_boundary > (0.9*length)) == False: print("Possible issue with right boundary: " + str(right_boundary))
    if (200<top_boundary<400) == False: print("Possible issue with upper boundary: " + str(top_boundary))
    if (600<lower_boundary<900) == False: print("Possible issue with lower boundary: " + str(lower_boundary))


    #double check y-axis for right market caps based on ticker locations
    top = all([gray_image[top_boundary][x] == 255 for x in range(left_boundary - 16, left_boundary)])
    bottom = all([gray_image[lower_boundary+1][x] == 255 for x in range(left_boundary - 16, left_boundary)])
    #if tickers are not on top and/or bottom, adjustments are needed
    if (top and bottom) == False:
    #get first two tickers from top to determine ticker interval length; tickers should be at least length 15
        num_tickers = 0
        last_ticker_y_coord = None
        ticker_y_locations = []
        for y in range(top_boundary, lower_boundary + 5):
            #ticker width can be up to 5(assumed); ignore the succeeding y-coords after finding a ticker
            if last_ticker_y_coord != None and y - last_ticker_y_coord < 5: continue
            ticker_check = all([gray_image[y][x] == 255 for x in range(left_boundary - 16, left_boundary)])
            if ticker_check:
                num_tickers += 1
                last_ticker_y_coord = y
                ticker_y_locations.append(y)
        ticker_distances = [ticker_y_locations[i+1] - ticker_y_locations[i] for i in range(len(ticker_y_locations) - 1)]
        average_ticker_interval = sum(ticker_distances) / len(ticker_distances) #tickers can have varying pixel distances of 1-5
        market_cap_ratio = (max_market_cap - min_market_cap) / len(ticker_distances) #market cap amount per ticker interval
        if top == False: #adjust upper market cap
            x = abs(top_boundary - ticker_y_locations[0]) / average_ticker_interval #distance reletive to a full ticker interval  
            max_market_cap = max_market_cap + (x * market_cap_ratio)
        if bottom == False: #adjust lower market cap
            x = abs(lower_boundary - ticker_y_locations[-1]) / average_ticker_interval #distance reletive to a full ticker interval  
            min_market_cap = min_market_cap - (x * market_cap_ratio)
    # print("Market caps: " + str((min_market_cap, max_market_cap)))


    #with calculated boundaries, crop the image and do final processing to get reference list
    cropped_image = gray_image[top_boundary:lower_boundary, left_boundary:right_boundary]
    length = cropped_image.shape[1]
    height = cropped_image.shape[0]

    pixel_coordinates = []
    for x in range(length):
        for y in range(height):
            if cropped_image[y,x] in [170, 132]: #is the value of green and red after being gray-scaled respectively
                pixel_coordinates.append([x,height - y]) #need to reverse y-value since it is read top-down
                break

    reference_list = []

    unix_added_date = int(datetime.strptime(start_date + " 16:00:00", "%B %d, %Y %H:%M:%S").timestamp())
    unix_removed_date = int(datetime.strptime(end_date + " 16:00:00", "%B %d, %Y %H:%M:%S").timestamp())
    x_slope = (unix_removed_date - unix_added_date) / length #new axis
    y_slope = (max_market_cap - min_market_cap) / height
    for i in range(len(pixel_coordinates)):
        temp_x_coord = pixel_coordinates[i][0]
        x_coord = x_slope * temp_x_coord + unix_added_date
        temp_y_coord = pixel_coordinates[i][1]
        y_coord = (y_slope * temp_y_coord) + min_market_cap
        reference_list.append([x_coord,y_coord])
    return reference_list


#return the min, max of the marketcap range in the finchat image 
    #checked multiple times; all default listed caps below are correct; no further changes should be needed
def get_finchat_market_cap_range(index):
    #store min and max of y-axis
    #will make code to double-check/adjust for screenshotted data
    finchat_download_dict = {545:(15,60), 670:(3,12), 680:(0,45)}
    finchat_screenshot_dict = {586:(0,40), 595:(5,20), 609:(0,40), 612:(0,15), 649:(5,25), 652:(10,30), 659:(0, 250), 660:(0,140) 
                            ,662:(2,7), 679:(0, 70), 691:(2,14), 695:(0,70)}
    #700s, fixed 767 image
    finchat_download_dict.update({721:(1,14), 727:(2,26), 737:(0,11), 743:(7,14), 754:(5,13), 762:(0,24), 765:(3.75,6.5)
                                ,767:(0,14), 769:(0,8), 771:(2,24), 782:(2,18), 784:(3,14)})
    finchat_screenshot_dict.update({714:(0,35), 716:(0,30), 718:(2,16), 731:(5,20), 733:(0,20), 744:(2,14), 747:(0,20), 749:(0,15)
                                    ,750:(5,20), 752:(0,10), 753:(0,40), 756:(10,35), 757:(5,20), 758:(0,15), 759:(0,35)
                                    ,761:(0,8), 770:(0,15), 774:(0,12), 775:(2,12), 776:(0.5,3), 780:(2,12), 781:(1,5)
                                    ,786:(2,16), 787:(2,8), 790:(10,40), 791:(3,9), 797:(0,15)})
    #800s
    finchat_download_dict.update({806:(3,11), 861:(0,4.5), 867:(6,15), 868:(0,35), 883:(1,8)})
    finchat_screenshot_dict.update({801:(2,12), 803:(0,10), 810:(4,16), 817:(2,14), 818:(2,12), 819:(20,100), 820:(0,120)
                                    ,821:(5,30), 822:(4,8), 824:(1,7), 826:(0,40), 827:(20,55), 828:(1,5), 835:(10,20), 838:(2,9)
                                    ,842:(5,40), 847:(5,30), 848:(1,4.5), 850:(0,10), 859:(2,6), 860:(10,30), 862:(1,5), 863:(1,5)
                                    ,864:(0,8), 866:(0,40), 872:(5,11), 873:(4,8), 874:(2,14), 876:(10,25), 877:(0,7), 879:(0,40)
                                    ,880:(5,20), 881:(4,16), 884:(4,14), 885:(15,30), 886:(0,30), 887:(4,7), 888:(1,2), 889:(2,10)
                                    ,890:(10,22), 891:(0,20), 892:(2,8), 895:(1,4), 898:(4,16)})
    #900s
    finchat_download_dict.update({901:(3,12), 903:(3,11), 904:(0.25,3.75), 905:(2,26), 910:(3,6.5), 911:(1.5,5), 914:(6,30)
                                ,915:(1,13), 916:(2,10), 917:(0,7), 918:(0,40), 919:(5,8.5), 922:(0,55), 923:(0,4), 925:(8,40)
                                ,927:(1,14), 931:(0,20), 932:(20,75), 933:(4,17), 937:(5,19), 939:(0.75,3.5), 941:(4,26)
                                ,943:(2,16), 947:(2,22), 948:(2,16), 949:(5,55), 950:(3,11), 952:(3,7.5), 953:(20,80), 955:(7,15)
                                ,956:(0,30), 958:(15,55), 960:(2,20), 962:(0.75,4), 968:(0.5,5), 969:(3,9), 973:(0,40), 982:(8,18)
                                ,989:(1.9,3.1), 990:(2,5), 993:(0.75,3.25), 994:(4,13), 995:(0.5,2.75), 997:(2.5,6.5), 999:(22,42)})
    finchat_screenshot_dict.update({900:(1,5), 912:(10,25), 921:(0,15), 924:(2,14), 930:(0,14), 936:(4,12), 938:(2,10), 942:(0,8)
                                    ,951:(0.5,4), 954:(0.5,4), 963:(0,2.5), 965:(10,35), 971:(12,20), 983:(0,14), 988:(2,7)})
    #1000s, fixed 1078
    finchat_download_dict.update({1000:(3,9), 1007:(5,14), 1011:(2,14), 1016:(2.5,6.5), 1017:(3,10), 1019:(4,22), 1022:(0.3,1.2)
                                ,1023:(0.3,1.3), 1025:(6,15), 1032:(3.8,5.6), 1033:(3,12), 1034:(2,20), 1036:(4,13), 1037:(3.5,6)
                                ,1038:(0.7,1.8), 1039:(6,10.5), 1040:(1.2,3.4), 1042:(10,21), 1043:(2.6,4.6), 1044:(0.7,1.7)
                                ,1046:(2.5,5.75), 1049:(1,7), 1052:(40,80), 1057:(2,8), 1060:(2.5,6.5), 1063:(1.75,5.25)
                                ,1064:(0.55,1.05), 1065:(2.25,5.25), 1066:(10,55), 1067:(16,34), 1073:(3.75,6.75), 1075:(3.5,8.5)
                                ,1078:(45,90), 1080:(1.4,3.2), 1081:(1,5.5), 1084:(0.4,1.3), 1085:(2.5,5.25), 1086:(0.7,2)
                                ,1087:(0.3,1.3), 1088:(40,90), 1089:(4,11), 1090:(7,18), 1091:(0,2.75), 1092:(3,11), 1094:(1.5,4)
                                ,1095:(0.2,1.8), 1097:(4,8), 1098:(5.5,10)})
    finchat_screenshot_dict.update({1001:(20,70), 1003:(10,25), 1006:(2,10), 1009:(0.6,1.8), 1010:(2.5,5), 1013:(1,7), 1014:(0.6,1.8)
                                    ,1018:(4,10), 1021:(1.5,5), 1045:(0.6,1.6), 1047:(5,40), 1050:(4,12), 1054:(1,5), 1059:(3,7)
                                    ,1061:(1,2.5), 1069:(0.5,2), 1079:(5,9), 1082:(1.5,5), 1083:(2,9), 1093:(0.015,0.05)})
    #1100s, fixed 1100, 1146
    finchat_download_dict.update({1100:(15,70), 1103:(2.5,5.5), 1105:(4,14), 1106:(0.7,1.6), 1107:(5,11), 1108:(2,6.5), 1109:(0.4,2.2)
                                ,1110:(5,13), 1111:(3,6), 1113:(1,3), 1114:(8,17), 1117:(36,60), 1119:(14,21), 1120:(0.25,0.8)
                                ,1125:(1.8,4), 1126:(17,29), 1127:(4,10), 1128:(4,10), 1129:(1.4,2.7), 1130:(28,52), 1131:(4.5,8.5)
                                ,1133:(2.1,3), 1136:(1.9,2.7), 1137:(2.1,3.4), 1138:(3,9), 1140:(2,6.5), 1142:(4.5,9.5)
                                ,1146:(1.52,1.72), 1147:(0.43,0.7)})
    finchat_screenshot_dict.update({1101:(40,120), 1102:(5,12), 1118:(1.5,3.5), 1121:(1,3), 1123:(40,90), 1132:(2,4.5), 1134:(3,5.5)
                                    ,1135:(7,14), 1139:(0.8,1.6), 1141:(2.8,3.8), 1143:(14,22), 1144:(2.2,3.2), 1145:(0.4,0.6)
                                    ,1150:(1.8,2.3)})

    market_cap_range = finchat_download_dict.get(index)
    if market_cap_range == None: market_cap_range = finchat_screenshot_dict.get(index)
    if market_cap_range == None: print("Error: Market cap range not found for finchat image: " + str(index)) 
    return market_cap_range