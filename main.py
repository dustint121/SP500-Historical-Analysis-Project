import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import requests
import json
from datetime import datetime
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
    sp_500_dict["Added_Date"][1044] = "January 2, 1996"
    # Missing added dates:    
        # {922 : DAN} :   #not in github; 
        # {1028 : BS} : #not found in github; not in tingo data either
        # {1046 : OC} : #not found in github
    #ENRNQ
    pd.DataFrame(sp_500_dict).to_csv("cleaned_sp_500_dataset.csv", index=False)


def get_company_data(tiingo_ticker, company_profile, company_name, meta_data_list, original_ticker, index):
    if original_ticker == "STI": #586
        company_profile["company_name"] = "SunTrust Banks Inc"
        company_profile["is_delisted"] = True
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["tiingo_meta_data_index"] = 16009
    elif original_ticker == "LLL": #597
        company_profile["company_name"] = "L3 Communications Holdings Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "Aerospace & Defense"
        company_profile["tiingo_meta_data_index"] = 10076 
    elif original_ticker == "CA": #611
        company_profile["company_name"] = "CA Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Infrastructure"
        company_profile["tiingo_meta_data_index"] = 2767        
    elif original_ticker == "XL": #612
        company_profile["company_name"] = "XL Group Ltd"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Insurance - Property & Casualty"
    elif original_ticker == "DPS": #614
        company_profile["company_name"] = "Dr Pepper Snapple Group Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Beverages - Non-Alcoholic"
    elif original_ticker == "WYND": #620
        company_profile["company_name"] = "Wyndham Worldwide Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Lodging"
    elif original_ticker == "DOW" and index == 629: #629
        company_profile["company_name"] = "Dow Chemical Company"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Chemicals"
    elif original_ticker == "SE": #652
        company_profile["company_name"] = "Spectra Energy Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Gas"   
    elif original_ticker == "HAR": #648
        company_profile["company_name"] = "Harman International Industries IncDE"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Consumer Electronics"         
    elif original_ticker == "EMC": #659
        company_profile["company_name"] = "EMC Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Hardware, Equipment & Parts"
    elif original_ticker == "FRX": #714
        company_profile["company_name"] = "Forest Laboratories Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Healthcare"
        company_profile["industry"] = "Biotechnology"
        company_profile["tiingo_meta_data_index"] = 6673 
    elif original_ticker == "LSI": #716
        company_profile["company_name"] = "LSI Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductors"
        company_profile["tiingo_meta_data_index"] = 10255
    elif original_ticker == "BEAM": #718
        company_profile["company_name"] = "Beam Suntory Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Beverages - Wineries & Distilleries"
    elif original_ticker == "JCP": #726
        company_profile["company_name"] = "JCPenney"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Department Stores"
    elif original_ticker == "NYX": #727
        company_profile["company_name"] = "NYSE Euronext"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Asset Management"
        company_profile["tiingo_meta_data_index"] = 12274
    elif original_ticker == "S": #732
        company_profile["company_name"] = "Sprint Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecom Services"
        company_profile["tiingo_meta_data_index"] = 14743
    elif original_ticker == "PGN": #750
        company_profile["company_name"] = "Progress Energy Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["tiingo_meta_data_index"] = 13127
    elif original_ticker == "NVLS": #752
        company_profile["company_name"] = "Novellus Systems Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
        company_profile["tiingo_meta_data_index"] = 12173
    elif original_ticker == "EP": #753
        company_profile["company_name"] = "El Paso Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas E&P"
        company_profile["tiingo_meta_data_index"] = 5708
    elif original_ticker == "CEG": #757
        company_profile["company_name"] = "Constellation Energy Group Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["tiingo_meta_data_index"] = 3179
    elif original_ticker == "CPWR": #758
        company_profile["company_name"] = "Compuware Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Services"
    elif original_ticker == "MI": #767
        company_profile["company_name"] = "Marshall & Ilsley Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["tiingo_meta_data_index"] = 10870
    elif original_ticker == "SII": #782
        company_profile["company_name"] = "Smith International"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Equipment & Services"
    elif original_ticker == "STR": #784
        company_profile["company_name"] = "Questar Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas E&P"
        company_profile["tiingo_meta_data_index"] = 16068
    elif original_ticker == "JAVA": #792
        company_profile["company_name"] = "Sun Microsystems Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Scientific & Technical Instruments"
        company_profile["tiingo_meta_data_index"] = 9200
    elif original_ticker == "DYN": #797
        company_profile["company_name"] = "Dynegy Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["tiingo_meta_data_index"] = 5235
    elif original_ticker == "PVN": #931
        company_profile["company_name"] = "Providian Financial Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
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
        ticker_exception_list = ["DAY","WAB","CPAY","CBOE","ORLY","BXP","EL","LH","MTB","YUM","BK","GE","IBM","SLB"]
        #500s
        ticker_exception_list += ["VFC","ZION","WU","DINO","ETFC","MAC","SIVBQ","GAP","FRCB","MRKT"]
        #600s
        ticker_exception_list += ["GT","GGP","DPS","BBBYQ","MNKKQ","TE","ADT","GMCR","CNX","ALTR1","PLL1","DTV1","ATI"] 
        #700s
        ticker_exception_list += ["WINMQ","LIFE2","DFODQ","BIGGQ","RRD","ATGE","SHLDQ","MMI1","SUNEQ","NSM1","Q1","ODP",
                                  "PTV","MIL1","XTO","BDK"] 

        if ticker in ticker_exception_list or (is_valid_exchange and is_right_company):
            company_profile["company_name"] = tiingo_data["name"].title()
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
        ticker_exception_list = []
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


def get_market_cap_data(ticker, original_ticker, index, added_date, removal_date):
    if original_ticker in ["INFO", "STI", "LLL"]: #500s
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return

    elif 600<=index<700 and original_ticker in ["CA","XL","MON","DOW","DNB","FTR","HAR","LLTC","SE","DO","HOT","EMC",
                                              "TYC","TE","CVC","ADT","BRCM","PCP","ALTR","PLL","NE","ATI"]: #600s
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return


    elif 700<=index<800 and original_ticker in ["BTU","FRX","LSI","BEAM","VIAV","MOLX","JCP","NYX","DELL","BMC","S","CBE"
                                                ,"SUN","LXK","EP","MMI","CEG","CPWR","TLAB","MWW","SUNEQ","NSM","MI"
                                                ,"Q","QLGC","MDP","STR","JAVA","DYN","SGP"
                                                ]:
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return
           

#NOTE: check 742, RRD, for market cap tiingo calcutions


    headers = {'Content-Type': 'application/json'}
    tiingo_URL = "https://api.tiingo.com/tiingo/fundamentals/" + ticker +"/daily?token=" + tiingo_token
    requestResponse = requests.get(tiingo_URL, headers=headers)
    tiingo_market_cap_data = requestResponse.json()
    tiingo_market_cap_data = [{"date":data["date"].split("T")[0],"marketCap":data["marketCap"]} 
                              for data in tiingo_market_cap_data]
    

    #get rid of possible market cap data with null market cap values at start
    for i, data in enumerate(tiingo_market_cap_data):
        if data["marketCap"] != None:
            tiingo_market_cap_data = tiingo_market_cap_data[i:]
            break


    if len(tiingo_market_cap_data) == 0:
        print("Empty tiingomarket cap data for: " + ticker)
    else: #double check that stock price data and market cap data are same size; else fix; test "CNW" stock
        tiingo_URL = ("https://api.tiingo.com/tiingo/daily/" + ticker + 
                    "/prices?startDate=1950-01-02&token=" + tiingo_token)
        requestResponse = requests.get(tiingo_URL, headers=headers)
        tiingo_stock_price_data = requestResponse.json()
        if (len(tiingo_stock_price_data) > len(tiingo_market_cap_data)):
            print("Incomplete markey cap data for tiingo. Fixing now: " + ticker)

            first_date_in_market_cap_data = datetime.strptime(tiingo_market_cap_data[0]["date"], "%Y-%m-%d")
            new_market_cap_data = []
            last_stock_market_cap_ratio = None
            for daily_stock_price_data in tiingo_stock_price_data:
                current_date = datetime.strptime(daily_stock_price_data["date"].split("T")[0], "%Y-%m-%d")
                if current_date >= first_date_in_market_cap_data:
                    last_stock_market_cap_ratio = tiingo_market_cap_data[0]["marketCap"] / daily_stock_price_data["close"]
                    if current_date != first_date_in_market_cap_data:
                        print("Same date match not found")
                    break
                new_market_cap_data.append(daily_stock_price_data)
            new_market_cap_data = [{"date": data["date"].split("T")[0]
                                    ,"marketCap": round(data["close"]*last_stock_market_cap_ratio, 2)} 
                              for data in new_market_cap_data]
            tiingo_market_cap_data = new_market_cap_data + tiingo_market_cap_data

            if tiingo_stock_price_data[-1]["date"].split("T")[0] != tiingo_market_cap_data[-1]["date"]:
                print("Tiingo data error: Ending Dates aren't the same: " + ticker)
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
    fmp_market_cap_data = [{"date":data["date"],"marketCap":data["marketCap"]} for data in fmp_market_cap_data]
    fmp_market_cap_data.reverse() #reverse the list to go from earliest to latest date like tiingo data

    tiingo_has_more_data = len(fmp_market_cap_data) <= len(tiingo_market_cap_data)
    if tiingo_has_more_data == False:
        print("Using fmp market data")

    market_cap_data = tiingo_market_cap_data if tiingo_has_more_data else fmp_market_cap_data


    #format: January 21, 2012
    added_date = datetime.strptime(added_date, "%B %d, %Y") 
    removal_date = (datetime.strptime(removal_date, "%B %d, %Y") if removal_date != None 
                    else datetime.strptime("September 30, 2024", "%B %d, %Y"))

    #get only needed data in list
    market_cap_data = [data for data in market_cap_data
                        if added_date<= datetime.strptime(data["date"], "%Y-%m-%d") <= removal_date
                    ]
    
    if len(market_cap_data) != 0:
        #format: 2006-01-31
        first_date_in_data = datetime.strptime(market_cap_data[0]["date"].split("T")[0], "%Y-%m-%d")
        last_date_in_data = datetime.strptime(market_cap_data[-1]["date"].split("T")[0], "%Y-%m-%d")

        start_of_1996 = datetime.strptime("January 1, 1996", "%B %d, %Y")
        if added_date < first_date_in_data and start_of_1996 < first_date_in_data:
            days_between = 0
            if added_date > start_of_1996:
                days_between = (first_date_in_data - added_date).days
            else:
                days_between = (first_date_in_data - start_of_1996).days
            print("Missing days of earlier data for market cap: " + str(days_between))
        if removal_date != None and last_date_in_data < removal_date:
            days_between = (removal_date - last_date_in_data).days
            print("Missing days of later data for market cap: " + str(days_between))


    if len(market_cap_data) < 500:
        if len(market_cap_data) == 0:
            print("No market cap data found: " + ticker)
        else:
            print("Less than 2 years of market cap data: " + ticker)





    file_path = "company_market_cap_data/" + str(index) + "_" + original_ticker + ".json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump(market_cap_data, file, indent=4)




# get_cleaned_sp_500_csv()

df = pd.read_csv("cleaned_sp_500_dataset.csv")
sp_500_dict = df.to_dict()
tickers = list(sp_500_dict["Ticker"].values())
company_names = list(sp_500_dict["Name"].values())
added_dates = list(sp_500_dict["Added_Date"].values())
removal_dates = list(sp_500_dict["Removed_Date"].values())

no_fmp_data_list = []
no_tiingo_data_list = []

for i, ticker in enumerate(tickers[:800]):
    if i < 750:
        continue
    # if i not in [89, 159, 272]: #FOX, NWS, GOOG #need fmp data
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
    with open("tiingo_meta_data.json", 'r') as file:
        meta_data_list = json.load(file)


    #some delisted or moved stocks have modified tickers for tiingo
    #500-600
    if 500<=i<600:
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
    #600-700    
    if 600<=i<700:
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
        if ticker == "SE":
            ticker == "SE1"
        if ticker == "ALTR":
            ticker = "ALTR1"
        if ticker == "PLL":
            ticker = "PLL1"
        if ticker == "DTV":
            ticker = "DTV1"
        if ticker == "WIN":
            ticker = "WINMQ"
    #700-800
    if 700<=i<800:
        if ticker == "LIFE":
            ticker = "LIFE2"
        if ticker == "BIG":
            ticker = "BIGGQ"
        if ticker == "DF":
            ticker = "DFODQ"
        if ticker == "ANR":
            ticker = "ANRZQ"
        if ticker == "SHLD":
            ticker = "SHLDQ"
        if ticker == "MMI":
            ticker= "MMI1"
        if ticker == "NSM":
            ticker = "NSM1"
        if ticker == "Q":
            ticker = "Q1"
        if ticker == "MIL":
            ticker = "MIL1"

    #800-900
    #900-1000

    #will get market cap data and place into file
    get_company_data(ticker, company_profile, company_name, meta_data_list, original_ticker, i)
    get_market_cap_data(ticker, original_ticker, i, added_date, removal_date)





