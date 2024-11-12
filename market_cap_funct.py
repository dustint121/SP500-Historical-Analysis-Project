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
                                ,951:(0.5,4), 954:(0.5,4), 963:(0,2.5), 965:(10,35), 970:(12,20), 983:(0,14), 988:(2,7)})










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

