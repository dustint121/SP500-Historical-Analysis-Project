import os
from dotenv import load_dotenv
import pandas as pd
import json
from datetime import datetime
from company_profile_func import *
from market_cap_func import * 
import pandas_market_calendars as mcal


# Actual keys is stored in a .env file.  Not good to store API key directly in script.
load_dotenv()
apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")

def get_sp_500_github_dataset():
    #this csv comes from this 3rd party project: https://github.com/fja05680/sp500/tree/master
    URL = "https://raw.githubusercontent.com/fja05680/sp500/refs/heads/master/S%26P%20500%20Historical%20Components%20%26%20Changes(08-17-2024).csv"
    df = pd.read_csv(URL)
    list_tickers = df["tickers"]
    for index in range(len(list_tickers)):
        list_tickers[index] = list_tickers[index].split(",")
    df["tickers"] = list_tickers
    df.to_pickle("github_datset.pkl")


def get_raw_sp_500_data_from_fmp():
    request = requests.get(f"https://financialmodelingprep.com/stable/sp500-constituent?apikey={apikey}")
    current_sp_list = request.json() # is a list of dicts/json objects
    current_sp_df = pd.DataFrame(columns=["Ticker", "Name", "Added_Date", "Removed_Date", "Replaces", "Removal_Reason"])
    for stock in current_sp_list:
        ticker = stock["symbol"]
        name = stock["name"]
        current_sp_df.loc[len(current_sp_df.index)] = [ticker, name, None, None, None, None]
    current_ticker_list = current_sp_df["Ticker"].to_list()
    #work in reverse to find the "added_date" of the current S&P stocks and "removed_date"
    date_list = ["October 1, 2024"]
    count = 0

    request = requests.get(f"https://financialmodelingprep.com/stable/historical-sp500-constituent?apikey={fmp_apikey}")
    historical_list = request.json() # is a list of dicts/json objects
    for stock in historical_list:
        date = stock["dateAdded"]
        added_ticker = stock["symbol"]
        added_company = stock["addedSecurity"]
        removed_ticker = stock["removedTicker"]
        removed_company = stock["removedSecurity"]
        reason = stock["reason"]
        if date not in date_list:
            date_list.append(date)
        if removed_ticker == "":
            removed_ticker = None
        if added_ticker == "BRK.B":
            added_ticker = "BRK-B"
        if added_ticker in current_ticker_list:
            count +=1
        #the date here is the day that a current stock was added; update "Added_Date"; should already be in df
        if added_ticker != None:
            current_sp_df.loc[(current_sp_df["Ticker"] == added_ticker) & (current_sp_df["Added_Date"].isnull() == True), 
                        ["Added_Date", "Replaces"]] = [date, removed_ticker]
        #the date here is also the date that a previous stock was removed; update "Removed_Date" this stock should not be in the df
        if removed_ticker != None:
            current_sp_df.loc[len(current_sp_df.index)] = [removed_ticker, removed_company, None, date, None, reason]
    current_sp_df.to_csv('raw_sp_500_dataset_from_fmp.csv', index=False)


def get_cleaned_sp_500_csv():
    df = pd.read_csv("raw_sp_500_dataset_from_fmp.csv")
    sp_500_dict = df.to_dict()
    sp_500_dict["Ticker"][541] = "UAA"  #UA.A -> UAA
    sp_500_dict["Ticker"][547] = "GAP" #updated ticker symbol
    sp_500_dict["Ticker"][557] = "DINO" #updated ticker symbol
    sp_500_dict["Name"][574] = "Harley-Davidson Inc."
    sp_500_dict["Ticker"][575] = "BFH" #new name and symbol
    sp_500_dict["Name"][575] = "Bread Financial Holdings, Inc." #new name and symbol
    sp_500_dict["Name"][579] = "Macy's, Inc."
    sp_500_dict["Ticker"][597] = "DD" #updated ticker symbol
    sp_500_dict["Name"][654] = "O-I Glass, Inc."
    sp_500_dict["Ticker"][664] = "CVC" #updated ticker symbol
    sp_500_dict["Ticker"][666] = "CCEP" #updated ticker symbol
    sp_500_dict["Name"][666] = "Coca-Cola Europacific Partners PLC"
    sp_500_dict["Ticker"][681] = "CB" #changed ticker symbol from ACE -> CB
    sp_500_dict["Ticker"][684] = "CMCSA" #for convience; Comcast had two classes of stock
    sp_500_dict["Ticker"][970] = "CMCSA" #for convience; Comcast had two classes of stock

                            
    sp_500_dict["Name"][686] = "Sigma-Aldrich Corporation"
    sp_500_dict["Name"][719] = "Cleveland-Cliffs Inc."
    sp_500_dict["Name"][738] = "T-Mobile US, Inc."
    sp_500_dict["Name"][799] = "Schering-Plough Corporation"
    sp_500_dict["Name"][827] = "Anheuser-Busch InBev SA/NV"
    sp_500_dict["Name"][857] = "TempleInland Inc"
    sp_500_dict["Name"][867] = "Archstone-Smith Trust"
    sp_500_dict["Name"][877] = "PMC-Sierra"
    sp_500_dict["Name"][900] = "AlbertoCulver Co"
    sp_500_dict["Name"][902] = "Louisiana-Pacific Corporation"
    sp_500_dict["Name"][906] = "KMG Chemicals, Inc."
    sp_500_dict["Name"][910] = "Knight Ridder, Inc."
    sp_500_dict["Name"][919] = "Jefferson Pilot Corporation"
    sp_500_dict["Name"][921] = "Scientific Atlanta, Inc."
    sp_500_dict["Name"][927] = "Georgia-Pacific Corporation"
    sp_500_dict["Name"][942] = "Power-One, Inc"
    sp_500_dict["Ticker"][976] = "INCLF" #changed ticker symbol from "N" -> "INCLF"
    sp_500_dict["Ticker"][977] = "GOLD"  #ticker symbol is from Canada, not U.S. "ABX" -> "GOLD"
    sp_500_dict["Ticker"][980] = "SHEL" #Royal Dutch Petroleum; changed ticker symbol from 'RD' to 'SHEL' #github as 'RDS'
    sp_500_dict["Name"][1029] = "O-I Glass, Inc."
    sp_500_dict["Ticker"][1093] = "MBWM" #changed ticker symbol from "MTL" -> "MBWM"
    sp_500_dict["Name"][1182] = "Conway Inc" #CNW
    sp_500_dict["Name"][1259] = "American Tower Corporation"
    sp_500_dict["Name"][1364] = "O-I Glass, Inc."
    sp_500_dict["Name"][1717] = "Archer-Daniels-Midland Company"


    sp_500_stocks_1996 = pd.read_pickle("github_dataset.pkl")["tickers"][0]
    for i in range(1200):
        ticker = sp_500_dict["Ticker"][i]
        added_date = sp_500_dict["Added_Date"][i]
        if added_date != added_date and ticker in sp_500_stocks_1996: #looking for nan values
            sp_500_dict["Added_Date"][i] = "January 2, 1996"
    #off by two for csv reading/indexing
    sp_500_dict["Added_Date"][506] = "July 1, 2015" # {'508:WRK} :  July 1, 2015 ; creation data and immediate entry into SP 500
    sp_500_dict["Added_Date"][600] = "March 4, 1957" # {602:GT}:  1957-03-04     #from article
    sp_500_dict["Added_Date"][602] = "March 4, 1957"  # {604:PCG} 
    sp_500_dict["Added_Date"][629] = "March 4, 1957" # {631:DOW}
    sp_500_dict["Added_Date"][630] = "March 4, 1957" # {632:DD}  : "1957-03-04"
    sp_500_dict["Added_Date"][646] = "March 1, 2001" # {648:FTR} : March 1, 2001;  #https://rbj.net/2001/02/22/citizens-to-join-sp-500/ 
    sp_500_dict["Added_Date"][651] = "March 4, 1957" # {653: PBI} : "1957-03-04"  #https://en.wikipedia.org/wiki/Pitney_Bowes 
    sp_500_dict["Added_Date"][656] = "March 4, 1957" # {658 : AA} : "1957-03-04"
    sp_500_dict["Added_Date"][660] = "January 2, 1996" # {662 : TYC} : "1996-01-01"   #estimate from wiki; bought out AMP and Raychem in 1999
    sp_500_dict["Added_Date"][681] = "July 1, 1976" #in test.csv, but under "CB" ticker; changed ticker symbol later on
    sp_500_dict["Added_Date"][703] = "January 1, 1964" # https://www.globalcosmeticsnews.com/avon-is-removed-from-the-s-p-500-after-50-years/ 
    sp_500_dict["Added_Date"][726] = "March 4, 1957" # {728 : JCP} : "1957-03-04"
    sp_500_dict["Added_Date"][735] = "March 4, 1957" # {737 : HNZ} : "1957-03-04"
    sp_500_dict["Added_Date"][749] = "January 1, 1986" # {751 : GR} was part of SP 500 by 1986 according to wiki
    sp_500_dict["Added_Date"][757] = "April 30, 1999" # {759 : CEG}  : "1999-04-30"; github dataset
    sp_500_dict["Added_Date"][777] = "March 4, 1957" # {779 : KODK}  :  "1957-03-04"
    sp_500_dict["Added_Date"][800] = "March 4, 1957" # {802 : WYE}   : "1957-03-04"
    sp_500_dict["Added_Date"][801] = "March 4, 1957" # {803 : CBE} :  "1957-03-04"
    sp_500_dict["Added_Date"][809] = "March 4, 1957" # {811 : GM} : "1957-03-04"
    sp_500_dict["Added_Date"][828] = "March 4, 1957" # {830 : HPC} : "1957-03-04"
    sp_500_dict["Added_Date"][913] = "January 2, 2001" #{915 : AMCC} : https://www.wsj.com/articles/SB977285516657763154
    sp_500_dict["Added_Date"][934] = "January 2, 1996" #{936: DAL} #github as 'DALRQ'
    sp_500_dict["Added_Date"][952] = "October 1, 1998" # {954 : UPC} : "1998-10-01" https://www.bizjournals.com/nashville/stories/1998/10/05/daily3.html
    sp_500_dict["Added_Date"][967] = "January 2, 1996" # {969 : AMR} #github as 'AAMRQ'
    sp_500_dict["Added_Date"][976] = "January 2, 1996" # github  INCLF
    sp_500_dict["Added_Date"][980] = "January 2, 1996" # {982 : RD}  #github as 'RDS'
    sp_500_dict["Added_Date"][1016] = "December 2, 1999" # {1018 : OK} : "1999-12-02" #from github
    sp_500_dict["Added_Date"][1045] = "January 2, 1996" # {1047: GAP} from github as GAPTQ
    sp_500_dict["Added_Date"][1046] = "November 3, 1998" # {1048 : NCE} : "1998-11-03" #from github
    sp_500_dict["Added_Date"][1053] = "January 2, 1996" # {1055 : MZ} from github as MZIAQ
    #below 3 are not confirmed, but used for convenience
    sp_500_dict["Added_Date"][920] = "January 2, 1996" # {922 : DAN} :  #not found in github
    sp_500_dict["Added_Date"][1026] = "January 2, 1996" # {1028 : BS} : #not found in github
    sp_500_dict["Added_Date"][1044] = "January 2, 1996" # {1046 : OC} : #not found in github
    pd.DataFrame(sp_500_dict).to_csv("cleaned_sp_500_dataset.csv", index=False)


def get_company_data(tiingo_ticker, company_profile, company_name, meta_data_list, original_ticker, index):
    misc_company_metadata = get_company_metadata_for_edge_cases(original_ticker,index)
    if misc_company_metadata != None:
        company_profile.update(misc_company_metadata)
    else:
        got_tiingo_data = get_tiingo_company_regular_data(tiingo_ticker, company_name, company_profile)
        got_tiingo_metadata = (get_tiingo_company_metadata(tiingo_ticker, company_profile, meta_data_list) 
                            if got_tiingo_data == True else False)
        if got_tiingo_metadata == False:
            get_fmp_metadata(original_ticker, company_name, company_profile, index)

    if company_profile.get("exchange") not in ["NYSE","NASDAQ"] : company_profile["exchange"] = get_stock_exchange_for_edge_cases(index)
    if company_profile.get("sector") == None or company_profile.get("industry") == None:
        print("Missing sector and industry in profile data: " + original_ticker)
    if company_profile.get("exchange") not in ["NYSE","NASDAQ"]:
        print("Missing exchange(NYSE, NASDAQ) in profile data: " + original_ticker)

    file_path = "company_profiles/" + str(i) + "_" + original_ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(company_profile, file, indent=4)


def get_market_cap_data(ticker, original_ticker, index, added_date, removal_date, company_name):
    marketcap_metadata = {"index":index,"ticker":original_ticker} #used to store where we get our data source
    market_cap_data = []

    #need to double-check that first day and last_day are valid (not a weekend or holiday); fix if needed
    date1 = max(datetime.strptime(added_date, "%B %d, %Y"), datetime.strptime("January 2, 1998", "%B %d, %Y")).__str__()[:10]
    date2 = (datetime.strptime("September 30, 2024", "%B %d, %Y").__str__()[:10] if removal_date == None
                       else datetime.strptime(removal_date, "%B %d, %Y").__str__()[:10])
    nyse = mcal.get_calendar('NYSE')
    valid_trading_days = nyse.valid_days(start_date=date1, end_date=date2)   
    first_day_needed = valid_trading_days[0].date().__str__()
    last_day_needed = valid_trading_days[-1].date().__str__()

    finchat_download_list = [545, 670, 680, 721, 727, 737, 743, 754, 762, 765, 767, 769, 771, 782, 784, 806, 861, 867, 868, 883, 901, 903, 904, 905, 910, 911, 914, 915, 916, 917, 918, 919, 922, 923, 925, 927, 931, 932, 933, 937, 939, 941, 943, 947, 948, 949, 950, 952, 953, 955, 956, 958, 960, 962, 968, 969, 973, 982, 989, 990, 993, 994, 995, 997, 999, 1000, 1007, 1011, 1016, 1017, 1019, 1022, 1023, 1025, 1032, 1033, 1034, 1036, 1037, 1038, 1039, 1040, 1042, 1043, 1044, 1046, 1049, 1052, 1057, 1060, 1063, 1064, 1065, 1066, 1067, 1073, 1075, 1078, 1080, 1081, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1094, 1095, 1097, 1098, 1100, 1103, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1113, 1114, 1117, 1119, 1120, 1125, 1126, 1127, 1128, 1129, 1130, 1131, 1133, 1136, 1137, 1138, 1140, 1142, 1146, 1147]
    finchat_screenshot_list = [117, 286, 586, 595, 609, 612, 649, 652, 659, 660, 662, 679, 691, 695, 714, 716, 718, 731, 733, 744, 747, 749, 750, 752, 753, 756, 757, 758, 759, 761, 770, 774, 775, 776, 780, 781, 786, 787, 790, 791, 797, 801, 803, 810, 817, 818, 819, 820, 821, 822, 824, 826, 827, 828, 835, 838, 842, 847, 848, 850, 859, 860, 862, 863, 864, 866, 872, 873, 874, 876, 877, 879, 880, 881, 884, 885, 886, 887, 888, 889, 890, 891, 892, 895, 898, 900, 912, 921, 924, 930, 936, 938, 942, 951, 954, 963, 965, 971, 983, 988, 1001, 1003, 1006, 1009, 1010, 1013, 1014, 1018, 1021, 1045, 1047, 1050, 1054, 1059, 1061, 1069, 1079, 1082, 1083, 1093, 1101, 1102, 1118, 1121, 1123, 1132, 1134, 1135, 1139, 1141, 1143, 1144, 1145, 1150]
    companiesmarketcap_list = [366, 377, 499, 618, 693, 726, 732, 766, 772, 792, 799, 800, 812, 839, 852, 869, 871, 893, 894, 897, 907, 929, 935, 940, 964, 966, 975, 985, 987, 996, 998, 1027, 1056, 1058, 1068, 1122]
    kibot_list = [620, 646, 648, 656, 657, 658, 664, 683, 711, 725, 735, 788, 804, 809, 853, 854, 855, 857, 926, 967, 1041, 1071, 1149]

    #edge case: 728 DELL
    if index == 728:
        market_cap_data = get_misc_market_cap_data(index, original_ticker, marketcap_metadata)
    #finchat.io
    elif index in ([1015, 1148, 1152] + finchat_download_list + finchat_screenshot_list):
        market_cap_data = get_finchat_market_cap_data(index, original_ticker, added_date, removal_date, marketcap_metadata)
    #companiesmarketcap.com
    elif index in companiesmarketcap_list:
        market_cap_data = get_companiesmarketcap_market_cap_data(index, added_date, removal_date, marketcap_metadata)
    #kibot.com
    elif index in kibot_list:
        market_cap_data = get_kibot_market_cap_data(index,original_ticker,added_date,removal_date, marketcap_metadata)
    #tiingo and fmp
    else:
        #format: January 21, 2012
        added_date = datetime.strptime(added_date, "%B %d, %Y") 
        removal_date = (datetime.strptime(removal_date, "%B %d, %Y") if removal_date != None 
                        else datetime.strptime("September 30, 2024", "%B %d, %Y"))
        min_date_needed = max(added_date,  datetime.strptime("1998-01-02", "%Y-%m-%d"))
        tiingo_market_cap_data = []
        fmp_market_cap_data = []
        #get market cap data from tiingo
        if get_tiingo_company_regular_data(ticker,company_name,{}):
            tiingo_market_cap_data = get_tiingo_market_cap_data(ticker, added_date,marketcap_metadata)
            #filter tiingo market cap data to filter out NULLs and unneeded dates
            tiingo_market_cap_data = [val for val in tiingo_market_cap_data 
                                    if val["market_cap"] != None and
                                    min_date_needed <= datetime.strptime(val["date"], "%Y-%m-%d") <= removal_date]
        #get fmp market cap data if tiingo data is empty or starts after "Added Date"
        if len(tiingo_market_cap_data) == 0 or marketcap_metadata.get("num_trading_days_to_calculate") not in [None,0]:
            if get_fmp_metadata(original_ticker,company_name,{},index):
                fmp_market_cap_data = get_fmp_market_cap_data(original_ticker,index)
                #filter fmp market cap data to filter out NULLs and unneeded dates
                fmp_market_cap_data = [val for val in fmp_market_cap_data
                                        if val["market_cap"] != None and
                                        min_date_needed <= datetime.strptime(val["date"], "%Y-%m-%d") <= removal_date]
        #decide whether to use fmp or tiingo data      
        tiingo_has_more_data = len(fmp_market_cap_data) <= len(tiingo_market_cap_data)
        num_days = len(fmp_market_cap_data) - len(tiingo_market_cap_data)
        if marketcap_metadata.get("num_trading_days_to_calculate"): #consider fmp, if tiingo has too many calculations done
            tiingo_has_more_data = (len(fmp_market_cap_data) < (len(tiingo_market_cap_data) - marketcap_metadata["num_trading_days_to_calculate"])
                                        or len(fmp_market_cap_data) == 0)
            num_days = len(fmp_market_cap_data) - (len(tiingo_market_cap_data) - marketcap_metadata["num_trading_days_to_calculate"])
        if tiingo_has_more_data == False:
            print("Using fmp market data; more by: " + str(num_days))
            marketcap_metadata["source"] = "fmp"
            if marketcap_metadata.get("num_trading_days_to_calculate"): del marketcap_metadata["num_trading_days_to_calculate"]
        else:
            marketcap_metadata["source"] = "tiingo"
        market_cap_data = tiingo_market_cap_data if tiingo_has_more_data else fmp_market_cap_data

    # add more data from investing.com for these 3 cases
    if index in [89,90,162]: 
        add_info = {}
        additional_marketcap_data = get_misc_market_cap_data(index, original_ticker, add_info)
        market_cap_data = additional_marketcap_data + market_cap_data
        marketcap_metadata["source"] += " + " + add_info["source"]

    #handle marketcap metadata
    if len(market_cap_data) > 0:
        marketcap_metadata["first_day_have_vs_needed"] = market_cap_data[0]["date"] + " : " + first_day_needed
        marketcap_metadata["last_day_have_vs_needed"] = market_cap_data[-1]["date"] + " : " + last_day_needed

        if type(added_date) == str:
            added_date = datetime.strptime(added_date, "%B %d, %Y") #format: January 1, 2005
        if type(removal_date) == str:    
            removal_date = datetime.strptime(removal_date, "%B %d, %Y") 
        
        first_date_in_data = datetime.strptime(market_cap_data[0]["date"], "%Y-%m-%d") #format: 2006-01-31
        last_date_in_data = datetime.strptime(market_cap_data[-1]["date"], "%Y-%m-%d")

        min_date_needed = datetime.strptime(first_day_needed, "%Y-%m-%d")
        if min_date_needed < first_date_in_data:
            missing_days = nyse.valid_days(start_date=min_date_needed, end_date=first_date_in_data)
            if str(min_date_needed.date()) == str(missing_days[0].date()): #exclude first day if same 
                missing_days = missing_days[1:]
            days_between = len(missing_days)
            if len(missing_days) > 0:
                print(first_day_needed + " : " + first_date_in_data.date().__str__())
                print("Missing days of earlier data for market cap: " + str(days_between))   
                marketcap_metadata["missing_num_days_before"] = days_between
        if last_date_in_data < datetime.strptime(last_day_needed, "%Y-%m-%d"):
            missing_days = nyse.valid_days(start_date=last_date_in_data, end_date=last_day_needed)
            if str(last_date_in_data.date()) == str(missing_days[0].date()): #exclude first day if same 
                missing_days = missing_days[1:]
            days_between = len(missing_days)
            if days_between > 0:
                print(last_date_in_data.date().__str__() + " : " + last_day_needed)
                print("Missing days of later data for market cap: " + str(days_between))
                marketcap_metadata["missing_num_days_after"] = days_between
    else:
        marketcap_metadata["is_empty"] = True
        print("No market cap data found: " + ticker)

    marketcap_metadata["num_of_days_data"] = len(market_cap_data)

    #store market cap data
    file_path = "company_market_cap_data/" + str(index) + "_" + original_ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(market_cap_data, file, indent=4)
    #store origin of market cap data
    file_path = "market_cap_metadata/" + str(index) + "_" + original_ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(marketcap_metadata, file, indent=4)



if __name__ == "__main__":
    # get_cleaned_sp_500_csv()
    df = pd.read_csv("cleaned_sp_500_dataset.csv")
    sp_500_dict = df.to_dict()
    tickers = list(sp_500_dict["Ticker"].values())
    company_names = list(sp_500_dict["Name"].values())
    added_dates = list(sp_500_dict["Added_Date"].values())
    removal_dates = list(sp_500_dict["Removed_Date"].values())

    for i, ticker in enumerate(tickers[:1153]):
        # if i < 500:
        #     continue
        # if i >= 500:
        #     continue 

        print(i)

        original_ticker = ticker
        added_date = added_dates[i]
        removal_date = removal_dates[i]
        if removal_date != removal_date: #for nan values
            removal_date = None

        company_profile = {}
        company_profile["ticker"] = ticker
        have_data_from_fmp = False
        company_name = company_names[i]

        meta_data_list = None
        with open("misc/tiingo_meta_data.json", 'r') as file:
            meta_data_list = json.load(file)


        #some delisted or moved stocks have modified tickers for tiingo
        #500-600
        if 500<=i<600:
            if ticker == "WRK":     ticker = "WRK-W"
            if ticker == "FRC":     ticker = "FRCB" 
            if ticker == "SIVB":    ticker = "SIVBQ" 
            if ticker == "STI":     ticker = "STI-WS-B"
            if ticker == "INFO":    ticker = "MRKT"
        #600-700    
        if 600<=i<700:
            if ticker == "XL":      ticker = "XLGLF"
            if ticker == "WYND":    ticker = "WYN"
            if ticker == "BBBY":    ticker = "BBBYQ"
            if ticker == "MNK":     ticker = "MNKKQ"
            if ticker == "ENDP":    ticker = "ENDPQ"
            if ticker == "SE":      ticker = "SE1"
            if ticker == "ALTR":    ticker = "ALTR1"
            if ticker == "PLL":     ticker = "PLL1"
            if ticker == "DTV":     ticker = "DTV1"
            if ticker == "WIN":     ticker = "WINMQ"
        #700-800
        if 700<=i<800:
            if ticker == "LIFE":    ticker = "LIFE2"
            if ticker == "DELL":    ticker = "DELL1"
            if ticker == "BIG":     ticker = "BIGGQ"
            if ticker == "DF":      ticker = "DFODQ"
            if ticker == "SUN":     ticker = "SUN1"
            if ticker == "ANR":     ticker = "ANRZQ"
            if ticker == "SHLD":    ticker = "SHLDQ"
            if ticker == "MMI":     ticker = "MMI1"
            if ticker == "NSM":     ticker = "NSM1"
            if ticker == "Q":       ticker = "Q1"
            if ticker == "MIL":     ticker = "MIL1"
        #800-900
        if 800<=i<900:
            if ticker == "CTX":     ticker = "CTX1"
            if ticker == "EQ":      ticker = "EQ1"
            if ticker == "WFT":     ticker = "WFTIQ"
            if ticker == "UST":     ticker = "UST1"
            if ticker == "WB":      ticker = "WB2"
            if ticker == "ABI":     ticker = "ABI1"
            if ticker == "BUD":     ticker = "BUD1"
            if ticker == "SAF":     ticker = "SAF2"
            if ticker == "OMX":     ticker = "OMX1"
            if ticker == "BSC":     ticker = "BSC1"
            if ticker == "CC":      ticker = "CCTYQ"
            if ticker == "CBH":     ticker = "CBH1"
            if ticker == "AT":      ticker = "AT1"
            if ticker == "AV":      ticker = "AV1"
            if ticker == "PD":      ticker = "PD1"
            if ticker == "FSLB":    ticker = "FSL-B"
        #900-1000
        if 900<=i<1000:
            if ticker == "ACV":     ticker = "ACV1"
            if ticker == "ASO":     ticker = "ASO1"
            if ticker == "EC":      ticker = "EC1"
            if ticker == "CHIR":    ticker = "CHIR1"
            if ticker == "BR":      ticker = "BR1"
            if ticker == "JP":      ticker = "JP1"
            if ticker == "DPH":     ticker = "DPHIQ"
            if ticker == "G":       ticker = "G1"
            if ticker == "SDS":     ticker = "SDS1"
            if ticker == "VRTS":    ticker = "VRTS1"
            if ticker == "S":       ticker = "S1"
            if ticker == "WLP":     ticker = "WLP1"
            if ticker == "CF":      ticker = "CF1"
            if ticker == "ONE":     ticker = "ONE1"
            if ticker == "AM":      ticker = "AM1"
            if ticker == "TAP.B":   ticker = "TAP-B"
            if ticker == "TUP":     ticker = "TUPBQ"
            if ticker == "CE":      ticker = "CE1"
            if ticker == "BGEN":    ticker = "BIIB"  #merger in 2003;reentered SP500 directly after
            if ticker == "HI":      ticker = "HI1"
            if ticker == "AMR":     ticker = "AAMRQ"
            if ticker == "COC.B":   ticker = "COC-B"
            if ticker == "NT":      ticker = "NRTLQ"
            if ticker == "AL":      ticker = "AL1"
            if ticker == "CNXT":    ticker = "CNXT1"
            if ticker == "U":       ticker = "UAIRQ"
            if ticker == "WCOM":    ticker = "WCOEQ"
            if ticker == "WLL":     ticker = "WLL1"
            if ticker == "NMK":     ticker = "NMK1"
            if ticker == "MEA":     ticker = "MEA1"
            if ticker == "TX":      ticker = "TX1"
        #1000-1100
        if 1000<=i<1100:
            if ticker == "WB":      ticker = "WB2" #also in 800s
            if ticker == "AGC":     ticker = "AGC1"
            if ticker == "AZA.A":   ticker = "AZA-A"
            if ticker == "CEN":     ticker = "CEN1"
            if ticker == "SUB":     ticker = "SUB1"
            if ticker == "UK":      ticker = "UK1"
            if ticker == "SMI":     ticker = "SMI1"
            if ticker == "VO":      ticker = "VO1"
            if ticker == "AFS.A":   ticker = "AFS-A"
            if ticker == "SEG":     ticker = "STX" #relisted in NASDAQ from NYSE
            if ticker == "CG":      ticker = "CG1"
            if ticker == "EFU":     ticker = "EFU1"
            if ticker == "BFO":     ticker = "BFO1"
            if ticker == "GAP":     ticker = "GAPTQ"
            if ticker == "RAD":     ticker = "RADCQ" #unneccessary, but for testing; has fmp data as well
            if ticker == "GTE":     ticker = "GTE1"
            if ticker == "MZ":      ticker = "MZIAQ"
            if ticker == "CHA":     ticker = "CHA1"
            if ticker == "UMG":     ticker = "USW" #incorrect or older symbol? using usw for something else 1051
            if ticker == "CSR":     ticker = "CSR1"
            if ticker == "TMC.A":   ticker = "TMC-A" 
            if ticker == "MIR":     ticker = "MIR1"
            if ticker == "RLM":     ticker = "RLM1"
            if ticker == "CBS":     ticker = "CBS1"
            if ticker == "ARC":     ticker = "ARC1"
            if ticker == "PBY":     ticker = "PBY1"
            if ticker == "FLE":     ticker = "FLTWQ"
            if ticker == "CSE":     ticker = "CSE1"
            if ticker == "AIT":     ticker = "AIT1"
            if ticker == "PHB":     ticker = "PHB1"
            if ticker == "FTL.A":   ticker = "FTL-A" 
            if ticker == "FRO":     ticker = "FRO1"
            if ticker == "NLC":     ticker = "NLC1"
            if ticker == "TA":      ticker = "TA2"
            if ticker == "PVT":     ticker = "PVT1"
        #1100-1200
        if 1100<=i<1200: 
            if ticker == "ATI":     ticker = "ATI1"
            if ticker == "ASND":    ticker = "ASND1"
            if ticker == "ASC":     ticker = "ASC1"
            if ticker == "HPH":     ticker = "HRZIQ"
            if ticker == "FMY":     ticker = "FMY1"
            if ticker == "UCC":     ticker = "UCC1"
            if ticker == "ANV":     ticker = "ANV1"
            if ticker == "AMP":     ticker = "AMP1"
            if ticker == "PZE":     ticker = "PZE1"
            if ticker == "CCI":     ticker = "CCI1"
            if ticker == "GSX":     ticker = "GSX1"
            if ticker == "FCN":     ticker = "FCN1"
            if ticker == "AHM":     ticker = "AHM1"
            if ticker == "DI":      ticker = "DI1"
            if ticker == "MNR":     ticker = "MNR1"
            if ticker == "BAY":     ticker = "BAY1"
            if ticker == "DIGI":    ticker = "DIGI2"
            if ticker == "GFS.A":   ticker = "GFS-A"
            if ticker == "ECH":     ticker = "ECH1"
            if ticker == "CHRS":    ticker = "CHRS1"
            if ticker == "SK":      ticker = "SK1"
            if ticker == "FLM":     ticker = "FLMIQ"

        get_company_data(ticker, company_profile, company_name, meta_data_list, original_ticker, i)
        # get_market_cap_data(ticker, original_ticker, i, added_date, removal_date, company_name)

