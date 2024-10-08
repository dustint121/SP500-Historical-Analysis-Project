import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import requests
import json
# Actual keys is stored in a .env file.  Not good to store API key directly in script.
load_dotenv()
apikey = os.environ.get("apikey")
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
    current_sp_list = fmpsdk.sp500_constituent(apikey=apikey)
    current_sp_df = pd.DataFrame(columns=["Ticker", "Name", "Added_Date", "Removed_Date", "Replaces", "Removal_Reason"])

    for stock in current_sp_list:
        ticker = stock["symbol"]
        name = stock["name"]
        current_sp_df.loc[len(current_sp_df.index)] = [ticker, name, None, None, None, None]

    current_ticker_list = current_sp_df["Ticker"].to_list()

    #work in reverse to find the "added_date" of the current S&P stocks and "removed_date"
    date_list = ["October 1, 2024"]
    count = 0
    historical_list = fmpsdk.historical_sp500_constituent(apikey=apikey)
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
    sp_500_dict["Ticker"][677] = "CEIX" #updated ticker symbol
    sp_500_dict["Ticker"][681] = "CB" #changed ticker symbol from ACE -> CB
    sp_500_dict["Name"][686] = "Sigma-Aldrich Corporation"
    sp_500_dict["Name"][719] = "Cleveland-Cliffs Inc."
    sp_500_dict["Name"][738] = "T-Mobile US, Inc."
    sp_500_dict["Name"][799] = "Schering-Plough Corporation"
    sp_500_dict["Name"][827] = "Anheuser-Busch InBev SA/NV"
    sp_500_dict["Name"][857] = "TempleInland Inc"
    sp_500_dict["Name"][867] = "Archstone-Smith Trust"
    sp_500_dict["Name"][877] = "PMC-Sierra"
    sp_500_dict["Name"][900] = "Virtus Diversified Income & Convertible Fund"
    sp_500_dict["Name"][902] = "Louisiana-Pacific Corporation"
    sp_500_dict["Name"][906] = "KMG Chemicals, Inc."
    sp_500_dict["Name"][910] = "Knight Ridder, Inc."
    sp_500_dict["Name"][919] = "Jefferson Pilot Corporation"
    sp_500_dict["Name"][921] = "Scientific Atlanta, Inc."
    sp_500_dict["Name"][927] = "Georgia-Pacific Corporation"
    sp_500_dict["Name"][942] = "Powerwave Technologies, Inc."
    sp_500_dict["Ticker"][976] = "INCLF" #changed ticker symbol from "N" -> "INCLF"
    sp_500_dict["Ticker"][980] = "SHEL" #Royal Dutch Petroleum; changed ticker symbol from 'RD' to 'SHEL' #github as 'RDS'
    sp_500_dict["Name"][1029] = "O-I Glass, Inc."
    sp_500_dict["Name"][1182] = "Conway Inc" #CNW
    sp_500_dict["Name"][1259] = "American Tower Corporation"
    sp_500_dict["Name"][1364] = "O-I Glass, Inc."
    sp_500_dict["Name"][1717] = "Archer-Daniels-Midland Company"
    #no name for {921 : JP}
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
    #below 3 are not confirmed, but used for converience
    sp_500_dict["Added_Date"][920] = "January 2, 1996"
    sp_500_dict["Added_Date"][1026] = "January 2, 1996"
    sp_500_dict["Added_Date"][104] = "January 2, 1996"
    # Missing added dates:    
        # {922 : DAN} :   #not in github; 
        # {1028 : BS} : #not found in github; not in tingo data either
        # {1046 : OC} : #not found in github
    #ENRNQ
    pd.DataFrame(sp_500_dict).to_csv("cleaned_sp_500_dataset.csv", index=False)


def get_company_data(tiingo_ticker, company_profile, company_name, meta_data_list, original_ticker, index):
    if original_ticker == "LLL":
        company_profile["company_name"] = "L3 Communications Holdings Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "Aerospace & Defense"
        company_profile["tiingo_meta_data_index"] = 16009  
    elif ticker == "CA":
        company_profile["company_name"] = "CA Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Infrastructure"
        company_profile["tiingo_meta_data_index"] = 2767         
    else:
        got_tingo_data = get_tiingo_company_regular_data(tiingo_ticker, company_name, company_profile)
        got_tingo_metadata = (get_tiingo_company_metadata(tiingo_ticker, company_profile, meta_data_list) 
                            if got_tingo_data == True else False)
        if got_tingo_metadata == False:
            get_fmp_metadata(original_ticker, company_name, company_profile)

    if company_profile.get("sector") == None or company_profile.get("industry") == None:
        print("Missing sector and industry in profile data: " + original_ticker)

    file_path = "company_profiles/" + str(i) + "_" + original_ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(company_profile, file, indent=4)


def get_tiingo_company_regular_data(ticker, company_name, company_profile):     
    headers = {'Content-Type': 'application/json'}
    tiingo_URL = "https://api.tiingo.com/tiingo/daily/" + ticker + "?token=" + tiingo_token
    tiingo_data= requests.get(tiingo_URL, headers=headers).json()
    if tiingo_data != {'detail': 'Not found.'}:
        is_valid_exchange = tiingo_data["exchangeCode"] in ["NASDAQ", "NYSE"]
        if is_valid_exchange == False and ticker in ["SBNY"]:   #later delisted from NYSE or NASDAQ
            is_valid_exchange = True
        company_name = company_name.lower().replace(',', '').replace('. ', ' ').replace('.', ' ').replace("'",'`')
        is_right_company = company_name[:5] in tiingo_data["name"].lower().replace('. ', ' ').replace('.', ' ')
        ticker_exception_list = ["DAY","WAB","CPAY","CBOE","ORLY","BXP","EL","LH","MTB","YUM","BK","GE","IBM","SLB"
                                 ,"VFC","ZION","WU","DINO","ETFC","MAC","SIVBQ","GAP","FRCB","MRKT"
                                 ,"GT","GGP","DPS","TE","ADT","GMCR","ATI"]
        if ticker in ticker_exception_list or (is_valid_exchange and is_right_company):
            company_profile["company_name"] = tiingo_data["name"]
            company_profile["is_delisted"] = "delisted" in tiingo_data["description"][:15].lower()
            company_profile["description"] = tiingo_data["description"].replace("DELISTED - ", '')
            return True
        else:
            if ticker not in []:
                print("Invalid Tiingo data for ticker symbol: " + ticker)
                return False
    else:
        print("No Tiingo data retrieved for: " + ticker)
        return False

    #DPS company data is not found on meta for tiingo or on FMP


def get_tiingo_company_metadata(ticker, company_profile, meta_data_list):
    tiingo_meta_data_index = {'A': 0, 'B': 1787, 'C': 2763, 'D': 4607, 'E': 5244, 'F': 6052, 'G': 6866, 
                            'H': 7687, 'I': 8349, 'J': 9170, 'K': 9383, 'L': 9730, 'M': 10382, 'N': 11485, 
                            'O': 12269, 'P': 12752, 'Q': 13899, 'R': 14030, 'S': 14729, 'T': 16316, 'U': 17321, 
                            'V': 17665, 'W': 18158, 'X': 18638, 'Y': 18773, 'Z': 18851}
    if ticker == "STI-WS-B":
        ticker = "STI" #two instances in meta data with same ticker, but we are looking at the first one
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["tiingo_meta_data_index"] = 16009

        return True

    first_letter = ticker[0]
    starting_index = tiingo_meta_data_index[first_letter]
    stopping_index = None
    if first_letter == 'Z':
        stopping_index = -1
    else:
        next_letter = list(tiingo_meta_data_index.keys())[list(tiingo_meta_data_index.keys()).index(first_letter) + 1]
        stopping_index = tiingo_meta_data_index[next_letter]
    meta_data_list = meta_data_list[starting_index:stopping_index]
    match_found = False
    for i, meta_data in enumerate(meta_data_list):
        if ticker.lower() == meta_data["ticker"]:
            company_profile["sector"] = meta_data["sector"]
            company_profile["industry"] = meta_data["industry"]
            company_profile["tiingo_meta_data_index"] = starting_index + i
            match_found = True
            return True
    if match_found == False:
        print("No tiingo meta data found for: " + ticker)
        return False


def get_fmp_metadata(ticker, company_name, company_profile):
    #only tested up to 600th stock in csv
    fmp_bio_list = fmpsdk.company_profile(apikey=apikey, symbol=ticker)
    if (len(fmp_bio_list) > 0):
        fmp_data = fmp_bio_list[0]
        is_valid_exchange = fmp_data["exchangeShortName"] in ["NASDAQ", "NYSE"]
        company_name = company_name.replace('. ', ' ').replace('.', ' ') #do not remove commas, just "."
        is_right_company = company_name.lower()[:5] in fmp_data["companyName"].lower().replace('. ', ' ').replace('.', ' ')
        # ticker_exception_list = ["WAB", "CBOE", "BXP", "LH", "BK", "GE", "IBM"
        #                          ,"ZION", "INFO", "DINO"]
        ticker_exception_list = []
        #name changes : WAB, CBOE, BXP, GE
        #SJM, DHI has unique grammer issues
        #BK, IBM shorthand name used
        if ticker in ticker_exception_list or (is_valid_exchange and is_right_company):
            have_data_from_fmp = True
            company_profile["company_name"] = fmp_data["companyName"]
            company_profile["sector"] = fmp_data["sector"]
            company_profile["industry"] = fmp_data["industry"]
            company_profile["is_delisted"] = not fmp_data["isActivelyTrading"]
            company_profile["description"] = fmp_data["description"]

            if fmp_data["sector"] in [None, ""]:
                print("Issue with fmp metadata: " + ticker)
                return False
            return True
        else: 
            print("Invalid FMP data for ticker symbol: " + ticker)
            return False

    else:
        # if ticker not in ["FLIR","VAR","CXO","TIF","NBL","ETFC","AGN","RTN","WCG","VIAB","CELG","TSS","APC","RHT"
        #                   ,"SCG"]:             
        print("No FMP data retrieved for: " + ticker)
        return False


def get_market_cap_data(ticker, original_ticker, index):
    if original_ticker in ["INFO", "STI", "LLL", "CA"]:
        print("Will need to make function to get market cap data from csv. Skip for now: " + ticker)
        return


    headers = {'Content-Type': 'application/json'}
    tiingo_URL = "https://api.tiingo.com/tiingo/fundamentals/" + ticker +"/daily?token=" + tiingo_token
    requestResponse = requests.get(tiingo_URL, headers=headers)
    tiingo_market_cap_data = requestResponse.json()
    if len(tiingo_market_cap_data) == 0:
        print("Empty tiingomarket cap data for: " + ticker)
    else: #double check that stock price data and market cap data are same size; else fix; test "CNW" stock
        tiingo_URL = ("https://api.tiingo.com/tiingo/daily/" + ticker + 
                    "/prices?startDate=1950-01-02&token=" + tiingo_token)
        requestResponse = requests.get(tiingo_URL, headers=headers)
        tiingo_stock_price_data = requestResponse.json()
        if (len(tiingo_stock_price_data) > len(tiingo_market_cap_data)):
            # print("Incomplete markey cap data for tiingo. Fixing now: " + ticker)
            new_market_cap_data = []
            market_cap_to_price_ratio = None
            if tiingo_market_cap_data[-1]["date"] == tiingo_stock_price_data[-1]["date"]:
                market_cap_to_price_ratio = tiingo_market_cap_data[-1]["marketCap"] / tiingo_stock_price_data[-1]["close"]
            else:
                print("Tiingo error: Ending Dates aren't the same: " + ticker)
                print(len(tiingo_stock_price_data))
                print(tiingo_market_cap_data[-1]["date"] + ": " + tiingo_stock_price_data[-1]["date"])
                return
            
            last_date_needed = tiingo_market_cap_data[-1]["date"]
            for val in tiingo_stock_price_data:
                current_date = val["date"]
                marketCap = round(val["close"] * market_cap_to_price_ratio, 2)
                if current_date == last_date_needed:
                    break
                new_market_cap_data.append({"date":current_date, "marketCap":marketCap,
                                    'enterpriseVal': None, 'peRatio': None, 'pbRatio': None, 'trailingPEG1Y': None})
            tiingo_market_cap_data = new_market_cap_data + tiingo_market_cap_data
    #get market cap data from fmp
    start_year = 2020
    end_year = 2024
    if index >  550:
        start_year = 2016
        end_year = 2020
    if index > 680:
        start_year = 2016
        end_year = 2012
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
    fmp_market_cap_data.reverse() #reverse the list to go from earliest to latest date like tiingo data

    tiingo_has_more_data = len(fmp_market_cap_data) <= len(tiingo_market_cap_data)
    market_cap_data = tiingo_market_cap_data if tiingo_has_more_data else fmp_market_cap_data

    if len(market_cap_data) < 500:
        if len(market_cap_data) == 0:
            print("No market cap data found: " + ticker)
        else:
            print("Less than 2 years of market cap data: " + ticker)





    file_path = "company_market_cap_data/" + str(i) + "_" + original_ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(market_cap_data, file, indent=4)













    # # return
    # file_path = "raw_tiingo_market_cap_files/" + str(index) + "_" + original_ticker + ".json"
    # os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # with open(file_path, 'w') as file:
    #     json.dump(market_cap_data, file, indent=4)
    # return



# get_cleaned_sp_500_csv()

df = pd.read_csv("cleaned_sp_500_dataset.csv")
sp_500_dict = df.to_dict()
tickers = list(sp_500_dict["Ticker"].values())
company_names = list(sp_500_dict["Name"].values())

no_fmp_data_list = []
no_tiingo_data_list = []

for i, ticker in enumerate(tickers[:300]):
    # if i < 500:
    #     continue

    if i not in [89, 159, 272]: #FOX, NWS, GOOG #need fmp data
        continue
    print(i)

    original_ticker = ticker

    #need to update, can find tiingo meta data
    # if ticker in ["LLL"]: #no data found whatsoever
    #     continue
    # if ticker in ["CA"]: #no data found whatsoever {611: CA}
    #     continue
    # if 600<i<650 and ticker == "DOW": #new company
    #     continue
    # if ticker in ["HAR"]:
    #     continue

#     #no data at all for two below
#     if ticker in ["SE"]:
#         continue
#     if ticker == "EMC":
#         continue

    company_profile = {}
    company_profile["ticker"] = ticker
    have_data_from_fmp = False
    company_name = company_names[i]

    meta_data_list = None
    with open("tiingo_meta_data.json", 'r') as file:
        meta_data_list = json.load(file)


    #some delisted or moved stocks have modified tickers for tiingo
    if ticker == "WRK":
        ticker = "WRK-W"
    if ticker == "FRC":
        ticker = "FRCB" 
    if ticker == "SIVB":
        ticker = "SIVBQ" 
    if ticker == "STI":
        ticker = "STI-WS-B"
    if ticker == "INFO":
        ticker = "MRKT"
    if ticker == "XL":
        ticker = "XLGLF"
    if ticker == "WYND":
        ticker = "WYN"
    if ticker == "BBBY":
        ticker = "BBBYQ"
    if ticker == "MNK":
        ticker = "MNKKQ"
    if ticker == "ENDP":
        ticker = "ENDPQ"
    if ticker == "ALTR":
        ticker = "ALTR1"
    if ticker == "PLL":
        ticker = "PLL1"
    if ticker == "DTV":
        ticker = "DTV1"
    if ticker == "WIN":
        ticker = "WINMQ"
    

    #will get market cap data and place into file
    get_company_data(ticker, company_profile, company_name, meta_data_list, original_ticker, i)
    get_market_cap_data(ticker, original_ticker, i)





