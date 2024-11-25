import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import json
from datetime import datetime
from company_profile_func import *
from market_cap_funct import * 
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
    if original_ticker == "STI" and index == 586: #586
        company_profile["company_name"] = "SunTrust Banks Inc"
        company_profile["is_delisted"] = True
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "LLL" and index == 595: #595
        company_profile["company_name"] = "L3 Communications Holdings Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "Aerospace & Defense"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "CA" and index == 611: #611
        company_profile["company_name"] = "CA Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Infrastructure"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "XL" and index == 612: #612
        company_profile["company_name"] = "XL Group Ltd"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Insurance - Property & Casualty"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "DPS" and index == 614: #614
        company_profile["company_name"] = "Dr Pepper Snapple Group Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Beverages - Non-Alcoholic"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "WYND" and index == 620: #620
        company_profile["company_name"] = "Wyndham Worldwide Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Lodging"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "DOW" and index == 629: #629
        company_profile["company_name"] = "Dow Chemical Company"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Chemicals"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "HAR" and index == 648: #648
        company_profile["company_name"] = "Harman International Industries IncDE"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Consumer Electronics"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "SE" and index == 652: #652
        company_profile["company_name"] = "Spectra Energy Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Gas"   
        company_profile["exchange"] = "NYSE"      
    elif original_ticker == "EMC" and index == 659: #659
        company_profile["company_name"] = "EMC Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Hardware, Equipment & Parts"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "FRX" and index == 714: #714
        company_profile["company_name"] = "Forest Laboratories Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Healthcare"
        company_profile["industry"] = "Biotechnology"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "LSI" and index == 716: #716
        company_profile["company_name"] = "LSI Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductors"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ" 
    elif original_ticker == "BEAM" and index == 718: #718
        company_profile["company_name"] = "Beam Suntory Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Beverages - Wineries & Distilleries"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "JCP" and index == 726: #726
        company_profile["company_name"] = "JCPenney"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Department Stores"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "NYX" and index == 727: #727
        company_profile["company_name"] = "NYSE Euronext"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Asset Management"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "S" and index == 732: #732
        company_profile["company_name"] = "Sprint Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecom Services"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "PGN" and index == 750: #750
        company_profile["company_name"] = "Progress Energy Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "NVLS" and index == 752: #752
        company_profile["company_name"] = "Novellus Systems Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "EP" and index == 753: #753
        company_profile["company_name"] = "El Paso Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas E&P"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "CEG" and index == 757: #757
        company_profile["company_name"] = "Constellation Energy Group Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE" #later iteration at index 42 is in NASDAQ
    elif original_ticker == "CPWR" and index == 758: #758
        company_profile["company_name"] = "Compuware Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Services"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "MI" and index == 767: #767
        company_profile["company_name"] = "Marshall & Ilsley Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "SII" and index == 782: #782
        company_profile["company_name"] = "Smith International"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Equipment & Services"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "STR" and index == 784: #784
        company_profile["company_name"] = "Questar Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas E&P"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "JAVA" and index == 792: #792
        company_profile["company_name"] = "Sun Microsystems Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Scientific & Technical Instruments"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "DYN" and index == 797: #797
        company_profile["company_name"] = "Dynegy Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "HPC" and index == 828: #828
        company_profile["company_name"] = "Hercules Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Specialty Chemicals"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "EDS" and index == 842: #842
        company_profile["company_name"] = "Electronic Data Systems"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Information Technology Services"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "TEK" and index == 861: #861
        company_profile["company_name"] = "Tektronix Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "ADCT" and index == 879: #879
        company_profile["company_name"] = "ADC Telecommunications Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "MEDI" and index == 880: #880
        company_profile["company_name"] = "MedImmune Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Healthcare"
        company_profile["industry"] = "Biotechnology"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "CMX" and index == 885: #885
        company_profile["company_name"] = "Caremark Rx Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Pharmaceutical Retailers"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "LU" and index == 897: #897
        company_profile["company_name"] = "Lucent Technologies Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "MERQ" and index == 924: #924
        company_profile["company_name"] = "Mercury Interactive Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Application"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "GP" and index == 927: #927
        company_profile["company_name"] = "Georgia-Pacific Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Paper & Paper Products"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "PVN" and index == 931: #931
        company_profile["company_name"] = "Providian Financial Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "PWER" and index == 942: #942
        company_profile["company_name"] = "Power-One, Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "PSFT" and index == 943: #943
        company_profile["company_name"] = "People Soft Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Information Technology Services"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "UPC" and index == 952: #952
        company_profile["company_name"] = "Union Planters Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "PCS" and index == 956: #956
        company_profile["company_name"] = "Sprint PCS Group"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecom Services"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "NSI" and index == 995: #995
        company_profile["company_name"] = "National Service Industries Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Personal Services"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "OAT" and index == 1007: #1007
        company_profile["company_name"] = "Quaker Oats Co"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Packaged Foods"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "H" and index == 1010: #1010
        company_profile["company_name"] = "Harcourt General Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Publishing"    
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "FPC" and index == 1032: #1032
        company_profile["company_name"] = "Florida Progress Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "COMS" and index == 1047: #1047
        company_profile["company_name"] = "3Com Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment"  
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "USW" and index == 1051: #1051, USW; need to specifiy index as well; being used by 1060, UMG
        company_profile["company_name"] = "US West Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecommunications Services" 
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "NLV" and index == 1074: #1074
        company_profile["company_name"] = "NextLevel Systems Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment" 
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "LI" and index == 1077: #1077
        company_profile["company_name"] = "Laidlaw International, Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "General Transportation" 
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "TEN" and index == 1083: #1083
        company_profile["company_name"] = "Tenneco Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Auto Parts"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "AR" and index == 1084: #1084
        company_profile["company_name"] = "Asarco Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Other Industrial Metals & Mining"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE"
    elif original_ticker == "SNT" and index == 1085: #1085
        company_profile["company_name"] = "Sonat Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Midstream"
        company_profile["source"] = "tiingo"  
        company_profile["exchange"] = "NYSE"     
    elif original_ticker == "BT" and index == 1105: #1105
        company_profile["company_name"] = "Bankers Trust Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = "tiingo"  
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "AN" and index == 1117: #1117
        company_profile["company_name"] = "Amoco Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Refining & Marketing"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "GRN" and index == 1119: #1119
        company_profile["company_name"] = "General Reinsurance Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Reinsurance industry"    
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "AS" and index == 1120: #1120
        company_profile["company_name"] = "Armco Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Steel"
        company_profile["source"] = "tiingo" 
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "STO" and index == 1121: #1121; succeeding company is SSCCQ
        company_profile["company_name"] = "Stone Container Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Paper & Paper Products"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "C" and index == 1122: #1122; use index, ticker being used by Citigroup now
        company_profile["company_name"] = "Chrysler Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Auto Manufacturers"
        company_profile["source"] = "tiingo"  
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "MCIC" and index == 1130: #1130
        company_profile["company_name"] = "MCI Communications Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecommunications Services"  
        company_profile["exchange"] = "NASDAQ"
    elif original_ticker == "MST" and index == 1133: #1133
        company_profile["company_name"] = "Mercantile Stores Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Department Stores"  
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "WAI" and index == 1134: #1134
        company_profile["company_name"] = "Western Atlas Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Equipment & Services"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "BNL" and index == 1138: #1138
        company_profile["company_name"] = "Beneficial Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Credit Services"
        company_profile["source"] = "tiingo"   
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "GNT" and index == 1140: #1140
        company_profile["company_name"] = "Green Tree Financial Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Mortgage Finance"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "PET" and index == 1141: #1141
        company_profile["company_name"] = "Pacific Enterprises"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Gas"
        company_profile["source"] = "tiingo" 
        company_profile["exchange"] = "NYSE"     
    elif original_ticker == "DEC" and index == 1142: #1142
        company_profile["company_name"] = "Digital Equipment Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Information Technology Services"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "CFL" and index == 1143: #1143
        company_profile["company_name"] = "Corestates Financial Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = "tiingo"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "FG" and index == 1144: #1144
        company_profile["company_name"] = "USF&G Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Insurance - Life"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "CBB" and index == 1150: #1150
        company_profile["company_name"] = "Caliber System Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "Integrated Freight & Logistics"
        company_profile["exchange"] = "NYSE" 
    elif original_ticker == "BBI" and index == 1152: #1152
        company_profile["company_name"] = "Barnett Banks Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"   
        company_profile["exchange"] = "NYSE" 
    else:
        got_tiingo_data = get_tiingo_company_regular_data(tiingo_ticker, company_name, company_profile)
        got_tiingo_metadata = (get_tiingo_company_metadata(tiingo_ticker, company_profile, meta_data_list) 
                            if got_tiingo_data == True else False)
        if got_tiingo_metadata == False:
            get_fmp_metadata(original_ticker, company_name, company_profile, index)

    exchange_dict = {129:"NASDAQ",527:"NASDAQ"}
    exchange_dict.update({635:"NASDAQ",636:"NYSE",650:"NASDAQ",662:"NYSE",683:"NASDAQ",691:"NYSE",692:"NASDAQ",699:"NYSE"})
    exchange_dict.update({721:"NASDAQ",728:"NASDAQ",736:"NYSE",739:"NYSE",744:"NYSE",748:"NASDAQ",754:"NYSE",762:"NYSE"
                          ,766:"NYSE",772:"NYSE",781:"NYSE",783:"NYSE",785:"NYSE",787:"NYSE"})
    exchange_dict.update({800:"NYSE",803:"NYSE",806:"NYSE",818:"NYSE",824:"NYSE",826:"NYSE",827:"NYSE",835:"NYSE"
                          ,838:"NYSE",840:"NYSE",841:"NYSE",848:"NYSE",853:"NYSE",854:"NYSE",859:"NYSE",860:"NYSE"
                          ,863:"NYSE",864:"NYSE",866:"NYSE",868:"NYSE",872:"NASDAQ",873:"NYSE",874:"NASDAQ",876:"NYSE"
                          ,886:"NYSE",888:"NYSE",889:"NASDAQ",890:"NYSE",892:"NYSE",893:"NYSE",898:"NYSE"})
    exchange_dict.update({901:"NYSE",907:"NYSE",909:"NYSE",911:"NYSE",915:"NASDAQ",916:"NYSE",918:"NYSE",923:"NYSE"
                          ,925:"NYSE",930:"NYSE",932:"NYSE",933:"NYSE",935:"NASDAQ",938:"NYSE",939:"NYSE",940:"NASDAQ"
                          ,941:"NYSE",947:"NYSE",948:"NASDAQ",949:"NYSE",953:"NYSE",954:"NYSE",958:"NYSE",960:"NYSE"
                          ,962:"NASDAQ",964:"NYSE",968:"NASDAQ",976:"NYSE",978:"NYSE",981:"NYSE",982:"NASDAQ",983:"NASDAQ"
                          ,988:"NYSE",990:"NYSE",991:"NYSE",993:"NYSE",994:"NYSE",996:"NYSE",997:"NYSE",999:"NYSE"})
    exchange_dict.update({1000:"NYSE",1003:"NYSE",1006:"NYSE",1015:"NYSE",1016:"NYSE",1017:"NYSE",1018:"NYSE",1019:"NYSE"
                          ,1022:"NYSE",1024:"NYSE",1027:"NYSE",1033:"NYSE",1035:"NYSE",1036:"NYSE",1037:"NYSE",1038:"NYSE"
                          ,1040:"NYSE",1043:"NYSE",1046:"NYSE",1048:"NYSE",1052:"NYSE",1053:"NYSE",1056:"NYSE",1057:"NYSE"
                          ,1058:"NYSE",1059:"NYSE",1061:"NYSE",1063:"NYSE",1064:"NYSE",1066:"NYSE",1067:"NYSE",1068:"NYSE"
                          ,1069:"NYSE",1071:"NYSE",1073:"NYSE",1075:"NYSE",1079:"NYSE",1081:"NYSE",1086:"NYSE",1087:"NYSE"
                          ,1088:"NYSE",1089:"NYSE",1094:"NYSE",1096:"NYSE",1097:"NYSE",1098:"NYSE",1099:"NYSE"})
    exchange_dict.update({1100:"NYSE",1101:"NASDAQ",1106:"NYSE",1107:"NYSE",1110:"NYSE",1112:"NASDAQ",1114:"NASDAQ"
                          ,1116:"NYSE",1118:"NYSE",1123:"NYSE",1124:"NYSE",1125:"NYSE",1126:"NYSE",1127:"NYSE",1128:"NYSE"
                          ,1129:"NYSE",1131:"NYSE",1132:"NASDAQ",1135:"NYSE",1146:"NYSE",1147:"NYSE"})
    if company_profile.get("exchange") not in ["NYSE","NASDAQ"] : company_profile["exchange"] = exchange_dict.get(index)
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
    kibot_list = [620, 646, 648, 656, 657, 658, 664, 683, 711, 725, 735, 743, 788, 804, 809, 853, 854, 855, 857, 926, 967, 1041, 1071, 1149]

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
        if i < 500:
            continue
        # if i >= 500:
        #     continue 

        # if i not in [1015, 1148, 1152]:
        #     continue

        finchat_download_list = [545, 670, 680, 721, 727, 737, 743, 754, 762, 765, 767, 769, 771, 782, 784, 806, 861, 867, 868, 883, 901, 903, 904, 905, 910, 911, 914, 915, 916, 917, 918, 919, 922, 923, 925, 927, 931, 932, 933, 937, 939, 941, 943, 947, 948, 949, 950, 952, 953, 955, 956, 958, 960, 962, 968, 969, 973, 982, 989, 990, 993, 994, 995, 997, 999, 1000, 1007, 1011, 1016, 1017, 1019, 1022, 1023, 1025, 1032, 1033, 1034, 1036, 1037, 1038, 1039, 1040, 1042, 1043, 1044, 1046, 1049, 1052, 1057, 1060, 1063, 1064, 1065, 1066, 1067, 1073, 1075, 1078, 1080, 1081, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1094, 1095, 1097, 1098, 1100, 1103, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1113, 1114, 1117, 1119, 1120, 1125, 1126, 1127, 1128, 1129, 1130, 1131, 1133, 1136, 1137, 1138, 1140, 1142, 1146, 1147]
        finchat_screenshot_list = [117, 286, 586, 595, 609, 612, 649, 652, 659, 660, 662, 679, 691, 695, 714, 716, 718, 731, 733, 744, 747, 749, 750, 752, 753, 756, 757, 758, 759, 761, 770, 774, 775, 776, 780, 781, 786, 787, 790, 791, 797, 801, 803, 810, 817, 818, 819, 820, 821, 822, 824, 826, 827, 828, 835, 838, 842, 847, 848, 850, 859, 860, 862, 863, 864, 866, 872, 873, 874, 876, 877, 879, 880, 881, 884, 885, 886, 887, 888, 889, 890, 891, 892, 895, 898, 900, 912, 921, 924, 930, 936, 938, 942, 951, 954, 963, 965, 971, 983, 988, 1001, 1003, 1006, 1009, 1010, 1013, 1014, 1018, 1021, 1045, 1047, 1050, 1054, 1059, 1061, 1069, 1079, 1082, 1083, 1093, 1101, 1102, 1118, 1121, 1123, 1132, 1134, 1135, 1139, 1141, 1143, 1144, 1145, 1150]
        # if i not in finchat_download_list:
        #     continue
        # if i not in finchat_screenshot_list:
        #     continue
        # if i not in ([1015, 1148, 1152] + finchat_download_list + finchat_screenshot_list):
        #     continue    

        companiesmarketcap_list = [366, 377, 499, 618, 693, 726, 732, 766, 772, 792, 799, 800, 812, 839, 852, 869, 871, 893, 894, 897, 907, 929, 935, 940, 964, 966, 975, 985, 987, 996, 998, 1027, 1056, 1058, 1068, 1122]
        # if i not in companiesmarketcap_list:
        #     continue
            

        kibot_list = [620, 646, 648, 656, 657, 658, 664, 683, 711, 725, 735, 743, 788, 804, 809, 853, 854, 855, 857, 926, 967, 1041, 1071, 1149]
        # if i not in kibot_list:
        #     continue

        misc_list = [89,90,162,728]
        # if i not in misc_list:
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

        # get_company_data(ticker, company_profile, company_name, meta_data_list, original_ticker, i)
        get_market_cap_data(ticker, original_ticker, i, added_date, removal_date, company_name)

    #NOTE: check 1048, RAD for market cap data; fmp has more data available and should be used if functioning correctly
        #for 1996-12-02, tiingo has $3.3 billion, while fmp has $5 billion
        #for 2001-06-28, tiingo has 3.5 billion, fmp has $4.2 billion, marketcap.com has $4.4 billion
        #for 2023-11-06, tiingo has $14.1 million, fmp has $13.7 million, marketcap.com has $36.8 million
