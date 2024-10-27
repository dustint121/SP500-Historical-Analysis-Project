import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd
import requests
import json
from datetime import datetime
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
    sp_500_dict["Name"][900] = "Virtus Diversified Income & Convertible Fund"
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
        company_profile["source"] = {"tiingo": 16009}
    elif original_ticker == "LLL": #597
        company_profile["company_name"] = "L3 Communications Holdings Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "Aerospace & Defense"
        company_profile["source"] = {"tiingo": 10076}
    elif original_ticker == "CA": #611
        company_profile["company_name"] = "CA Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Infrastructure"
        company_profile["source"] = {"tiingo": 2767} 
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
        company_profile["source"] = {"tiingo": 6673}
    elif original_ticker == "LSI": #716
        company_profile["company_name"] = "LSI Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductors"
        company_profile["source"] = {"tiingo": 10255}
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
        company_profile["source"] = {"tiingo": 12274}
    elif original_ticker == "S": #732
        company_profile["company_name"] = "Sprint Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecom Services"
        company_profile["source"] = {"tiingo": 14743}
    elif original_ticker == "PGN": #750
        company_profile["company_name"] = "Progress Energy Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = {"tiingo": 13127}
    elif original_ticker == "NVLS": #752
        company_profile["company_name"] = "Novellus Systems Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
        company_profile["source"] = {"tiingo": 12173}
    elif original_ticker == "EP": #753
        company_profile["company_name"] = "El Paso Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas E&P"
        company_profile["source"] = {"tiingo": 5708}
    elif original_ticker == "CEG": #757
        company_profile["company_name"] = "Constellation Energy Group Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = {"tiingo": 3179}
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
        company_profile["source"] = {"tiingo": 10870}
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
        company_profile["source"] = {"tiingo": 16068}
    elif original_ticker == "JAVA": #792
        company_profile["company_name"] = "Sun Microsystems Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Scientific & Technical Instruments"
        company_profile["source"] = {"tiingo": 9200}
    elif original_ticker == "DYN": #797
        company_profile["company_name"] = "Dynegy Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = {"tiingo": 5235}
    elif original_ticker == "HPC": #828
        company_profile["company_name"] = "Hercules Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Specialty Chemicals"
        company_profile["source"] = {"tiingo": 8121}
    elif original_ticker == "EDS": #842
        company_profile["company_name"] = "Electronic Data Systems"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Information Technology Services"
    elif original_ticker == "TEK": #861
        company_profile["company_name"] = "Tektronix Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
        company_profile["source"] = {"tiingo": 16520}
    elif original_ticker == "ADCT": #879
        company_profile["company_name"] = "ADC Telecommunications Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment"
        company_profile["source"] = {"tiingo": 310}
    elif original_ticker == "MEDI": #880
        company_profile["company_name"] = "MedImmune Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Healthcare"
        company_profile["industry"] = "Biotechnology"
        company_profile["source"] = {"tiingo": 10720}
    elif original_ticker == "CMX": #885
        company_profile["company_name"] = "Caremark Rx Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Pharmaceutical Retailers"
        company_profile["source"] = {"tiingo": 3779}
    elif original_ticker == "LU": #897
        company_profile["company_name"] = "Lucent Technologies Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment"
        company_profile["source"] = {"tiingo": 10302}
    elif original_ticker == "MERQ": #924
        company_profile["company_name"] = "Mercury Interactive Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Software - Application"
        company_profile["source"] = {"tiingo": 10760}
    elif original_ticker == "GP": #927
        company_profile["company_name"] = "Georgia-Pacific Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Paper & Paper Products"
    elif original_ticker == "PVN": #931
        company_profile["company_name"] = "Providian Financial Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
    elif original_ticker == "PWER": #942
        company_profile["company_name"] = "Power-One, Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Semiconductor Equipment & Materials"
    elif original_ticker == "PSFT": #943
        company_profile["company_name"] = "People Soft Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Information Technology Services"
    elif original_ticker == "UPC": #952
        company_profile["company_name"] = "Union Planters Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = {"tiingo": 17529}
    elif original_ticker == "PCS": #956
        company_profile["company_name"] = "Sprint PCS Group"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecom Services"
    elif original_ticker == "NSI": #995
        company_profile["company_name"] = "National Service Industries Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Personal Services"
        company_profile["source"] = {"tiingo": 12029}
    elif original_ticker == "OAT": #1007
        company_profile["company_name"] = "Quaker Oats Co"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Defensive"
        company_profile["industry"] = "Packaged Foods"
    elif original_ticker == "H": #1010
        company_profile["company_name"] = "Harcourt General Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Publishing"    
    elif original_ticker == "FPC": #1032
        company_profile["company_name"] = "Florida Progress Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Electric"
        company_profile["source"] = {"tiingo": 6594}
    elif original_ticker == "COMS": #1047
        company_profile["company_name"] = "3Com Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment"  
        company_profile["source"] = {"tiingo": 3954}
    elif original_ticker == "USW" and index == 1051: #1051, USW; need to specifiy index as well; being used by 1060, UMG
        company_profile["company_name"] = "US West Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecommunications Services"  
    elif original_ticker == "NLV": #1074
        company_profile["company_name"] = "NextLevel Systems Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Communication Equipment" 
    elif original_ticker == "LI": #1077
        company_profile["company_name"] = "Laidlaw International, Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "General Transportation" 
    elif original_ticker == "TEN": #1083
        company_profile["company_name"] = "Tenneco Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Auto Parts"
        company_profile["source"] = {"tiingo": 16536}
    elif original_ticker == "AR": #1084
        company_profile["company_name"] = "Asarco Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Other Industrial Metals & Mining"
        company_profile["source"] = {"tiingo": 1192}
    elif original_ticker == "SNT": #1085
        company_profile["company_name"] = "Sonat Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Midstream"
        company_profile["source"] = {"tiingo": 15643}        
    elif original_ticker == "BT": #1105
        company_profile["company_name"] = "Bankers Trust Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = {"tiingo": 2603}   
    elif original_ticker == "AN": #1117
        company_profile["company_name"] = "Amoco Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Refining & Marketing"
        company_profile["source"] = {"tiingo": 1328}  
    elif original_ticker == "GRN": #1119
        company_profile["company_name"] = "General Reinsurance Corporation"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Reinsurance industry"    
    elif original_ticker == "AS": #1120
        company_profile["company_name"] = "Armco Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Steel"
        company_profile["source"] = {"tiingo": 1328}   
    elif original_ticker == "STO": #1121; succeeding company is SSCCQ
        company_profile["company_name"] = "Stone Container Corp."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Basic Materials"
        company_profile["industry"] = "Paper & Paper Products"
    elif original_ticker == "C" and index == 1122: #1122; use index, ticker being used by Citigroup now
        company_profile["company_name"] = "Chrysler Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Auto Manufacturers"
        company_profile["source"] = {"tiingo": 2766}   
    elif original_ticker == "MCIC": #1130
        company_profile["company_name"] = "MCI Communications Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Communication Services"
        company_profile["industry"] = "Telecommunications Services"  
    elif original_ticker == "MST": #1133
        company_profile["company_name"] = "Mercantile Stores Inc"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Department Stores"  
    elif original_ticker == "WAI": #1134
        company_profile["company_name"] = "Western Atlas Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Equipment & Services"
        company_profile["source"] = {"tiingo": 18191}
    elif original_ticker == "BNL": #1138
        company_profile["company_name"] = "Beneficial Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Credit Services"
        company_profile["source"] = {"tiingo": 2350}     
    elif original_ticker == "GNT": #1140
        company_profile["company_name"] = "Green Tree Financial Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Mortgage Finance"
    elif original_ticker == "PET": #1141
        company_profile["company_name"] = "Pacific Enterprises"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Utilities"
        company_profile["industry"] = "Utilities - Regulated Gas"
        company_profile["source"] = {"tiingo": 13057}      
    elif original_ticker == "DEC": #1142
        company_profile["company_name"] = "Digital Equipment Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Technology"
        company_profile["industry"] = "Information Technology Services"
    elif original_ticker == "CFL": #1143
        company_profile["company_name"] = "Corestates Financial Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"
        company_profile["source"] = {"tiingo": 3287}   
    elif original_ticker == "FG": #1144
        company_profile["company_name"] = "USF&G Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Insurance - Life"
    elif original_ticker == "CBB": #1150
        company_profile["company_name"] = "Caliber System Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Industrials"
        company_profile["industry"] = "Integrated Freight & Logistics"
    elif original_ticker == "BBI": #1152
        company_profile["company_name"] = "Barnett Banks Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Banks - Regional"   
    elif original_ticker == "HFS": #1154
        company_profile["company_name"] = "HFS Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Real Estate"
        company_profile["industry"] = "Real Estate Services"   
    elif original_ticker == "SB": #1156
        company_profile["company_name"] = "Salomon Brothers, Inc."
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Financial Services"
        company_profile["industry"] = "Asset Management"
    elif original_ticker == "LLX": #1157
        company_profile["company_name"] = "Louisiana Land & Exploration Co"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas E&P"
    elif original_ticker == "SRR": #1158
        company_profile["company_name"] = "Stride Rite Corp"
        company_profile["is_delisted"] = True
        company_profile["description"] = None
        company_profile["sector"] = "Consumer Cyclical"
        company_profile["industry"] = "Footwear & Accessories"
        company_profile["source"] = {"tiingo": 15888}    

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
    if company_name == None: return None

    headers = {'Content-Type': 'application/json'}
    tiingo_URL = "https://api.tiingo.com/tiingo/daily/" + ticker + "?token=" + tiingo_token
    tiingo_data= requests.get(tiingo_URL, headers=headers).json()
    if tiingo_data != {'detail': 'Not found.'}:
        is_valid_exchange = tiingo_data["exchangeCode"] in ["NASDAQ", "NYSE"]
        description = (tiingo_data["description"].replace(',', '').replace('. ', ' ').replace('.', ' ').lower() 
                        if tiingo_data["description"] != None else "")

        if is_valid_exchange == False and ticker in ["SBNY"]:   #later delisted from NYSE or NASDAQ
            is_valid_exchange = True
            
        company_name = (company_name.lower().replace(',', '').replace('. ', ' ').replace('.', ' ').replace("'",'`')
                        .replace('(',"").replace(')',"").strip())

        is_right_company = all([val in ["inc","co","corp","corporation","company","companies","the","-","&",
                                        "int`l","plc","ltd","llc"] 
                                or val in tiingo_data["name"].lower() or val in description.split()[:20]
                                for val in company_name.split()])

        ticker_exception_list = ["CPAY","CBOE","ORLY","DOC","BXP","EL","LH","BF-B","GE","NSC","SLB"]
        #500s
        ticker_exception_list += ["XOM","FRCB","SIVBQ","DISCK","MRKT","DINO","AGN"]
        #600s
        ticker_exception_list += ["GGP","LVLT","DD","BBBYQ","MNKKQ","FTR","ENDPQ","TE","VAL","CNX","ALTR1"
                                  ,"CMCSA","PLL1","DTV1"]
        #700s
        ticker_exception_list += ["LIFE2","DELL1","FHN","DFODQ","BIGGQ","RRD","SUN1","ATGE","SHLDQ","MMI1","SUNEQ","NSM1"
                                  ,"Q1","PTV","MIL1","XTO","BDK","SGP","WINMQ"]
        #800s
        ticker_exception_list += ["WYE","CTX1","EQ1","ROH","SOV","UST1","AW","ABI1","ASH","WWY","WEN","SAF2","FNMA","FMCC"
                                  ,"IAC","OMX1","CCTYQ","CBH1","CZR","DJ","AT1","BOL","AV1","TXU","ASN","SLR","CBSS","KSE"
                                  ,"BMET","MEL","PD1","PGL","APCC","EOP","SBL","BLS","NFB"]
        #900s
        ticker_exception_list += ["FSH","GTW","CTB","EC1","ABS","CHIR1","CIN","MYG","BR1","RBK","KRB","DPHIQ","G1","MAY"
                                  ,"NXTL","TOY","GLK","VRTS1","WLP1","SOTR","AWE","ONE1","AM1","FBF","CE1","BIIB","QTRN"
                                  ,"PHA","EHC","RATL","INCLF","AL1","SHEL","PDG","IMNX","CNXT1","WLL1","MEA1","KM","HM"
                                  ,"RAL","ENRNQ","GPU","TX1"]
        #1000s
        ticker_exception_list += ["TOS","WB2","AGC1","ETS","CEN1","OK","SUB1","UK1","CGP","SMI1","PRD","VO1","FJ","ACKH"
                                  ,"PWJ","CG1","EFU1","UCM","MKG","YNR","NCE","RADCQ","GTE1","MZIAQ","WLA","CHA1","USW",
                                  "CSR1","TMC-A","SMS","MIR1","JOS","CBS1","ARC1","PNU","PBY1","FLTWQ","CNG","RNB","PPW",
                                  "CSE1","CYM","DGN","AIT1","PHB1","MBWM","RYC","NLC1","BFI","TA2","PVT1"]
        #1100s
        ticker_exception_list += ["ATI1","ASND1","MWI","FMY1","AMP1","TCOMA","HBOC","SAI","PZE1","CCI1","GSX1","USS","FCN1"
                                  ,"AHM1","DI1","MNR1","BAY1","DIGI2","WMX","SK1","JH","BEV","SFS1","INGR1"]

        if ticker in ticker_exception_list or (is_valid_exchange and is_right_company):
            company_profile["company_name"] = tiingo_data["name"].title()
            company_profile["is_delisted"] = "delisted" in tiingo_data["description"][:15].lower()
            company_profile["description"] = tiingo_data["description"].replace("DELISTED - ", '')
            if company_profile["description"] == tiingo_data["name"]:
                company_profile["description"] = None
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
    
    #weird edge cases
    if ticker == "MDR":
        company_profile["sector"] = "Energy"
        company_profile["industry"] = "Oil & Gas Equipment & Services"
        company_profile["source"] = {"tiingo": None}
        return True
    if ticker == "MYG": ticker = "MYG1"
    if ticker == "MII": ticker = "MII1"


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
            company_profile["source"] = {"tiingo": starting_index + i}
            match_found = True
            return True
    if match_found == False:
        print("No tiingo meta data found for: " + ticker)
        return False


def get_fmp_metadata(ticker, company_name, company_profile):
    fmp_bio_list = fmpsdk.company_profile(apikey=apikey, symbol=ticker)
    if (len(fmp_bio_list) > 0):
        fmp_data = fmp_bio_list[0]
        is_valid_exchange = fmp_data["exchangeShortName"] in ["NASDAQ", "NYSE"]
        company_name = company_name.replace('. ', ' ').replace('.', ' ') #do not remove commas, just "."
        is_right_company = company_name.lower()[:8] in fmp_data["companyName"].lower().replace('. ', ' ').replace('.', ' ')
        ticker_exception_list = []
        if ticker in ticker_exception_list or (is_valid_exchange and is_right_company):
            have_data_from_fmp = True
            company_profile["company_name"] = fmp_data["companyName"]
            company_profile["sector"] = fmp_data["sector"]
            company_profile["industry"] = fmp_data["industry"]
            company_profile["is_delisted"] = not fmp_data["isActivelyTrading"]
            company_profile["description"] = fmp_data["description"]
            company_profile["source"] = "fmp"

            if fmp_data["sector"] in [None, ""]:
                print("Issue with fmp metadata: " + ticker)
                return False
            return True
        else: 
            print("Invalid FMP data for ticker symbol: " + ticker)
            return False

    else:        
        print("No FMP data retrieved for: " + ticker)
        return False

#get market cap data from fmp
def get_fmp_market_cap_data(original_ticker, index):
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
    return fmp_market_cap_data



#get market cap data from tiingo
def get_tiingo_market_cap_data(ticker, added_date):
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
    else: 
        #if market cap data already has enough data, return it early; else, look into stock price data
        first_date_in_market_cap_data = datetime.strptime(tiingo_market_cap_data[0]["date"], "%Y-%m-%d")
        if (first_date_in_market_cap_data <= added_date 
            or first_date_in_market_cap_data <= datetime.strptime("1996-01-02", "%Y-%m-%d")):
            return tiingo_market_cap_data

        print("Looking at tiingo stock price data.")
        tiingo_URL = ("https://api.tiingo.com/tiingo/daily/" + ticker + 
                    "/prices?startDate=1950-01-02&token=" + tiingo_token)
        requestResponse = requests.get(tiingo_URL, headers=headers)
        tiingo_stock_price_data = requestResponse.json()
        #double check that stock price data and market cap data are same size; else fix; test "CNW" stock
        missing_data_amount = len(tiingo_stock_price_data) - len(tiingo_market_cap_data)
        if (len(tiingo_stock_price_data) > len(tiingo_market_cap_data)):
            # print("Incomplete market cap data for tiingo. Fixing now: " + ticker)
            # if missing_data_amount > 1000:
            #     print(">=5 years of stock price data to add to tingo market cap: " + ticker)
            if (first_date_in_market_cap_data - added_date).days > 365:
                print("Missing days of data in initial tiingo market cap data: " + 
                    str((first_date_in_market_cap_data - added_date).days))
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

    return tiingo_market_cap_data



def get_market_cap_data(ticker, original_ticker, index, added_date, removal_date, company_name):
    if original_ticker in ["INFO", "STI", "LLL"]: #500s
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return
    elif 600<=index<700 and original_ticker in ["CA","XL","MON","DOW","DNB","FTR","HAR","LLTC","SE","DO","HOT","EMC",
                                              "TYC","TE","CVC","ADT","BRCM","PCP","ALTR","PLL","NE"]: #600s
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return
    elif 700<=index<800 and original_ticker in ["BTU","FRX","LSI","BEAM","VIAV","MOLX","JCP","NYX","DELL","BMC","S","CBE"
                                                ,"SUN","LXK","EP","MMI","CEG","CPWR","TLAB","MWW","SUNEQ","NSM","MI"
                                                ,"Q","QLGC","MDP","STR","JAVA","DYN","SGP"
                                                ]:
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return
    elif 800<=index<900 and original_ticker in ["WYE","CBE","EQ","GM","ROH","NE","UST","AW","ABI","BUD","HPC","WWY"
                                                ,"COOP","LEHMQ","EDS","BSC","CC","CBH","CZR","DJ","TEK","TXU","NCR"
                                                ,"FDC","CBSS","KSE","BMET"]:
        print("Will need to make function to get market cap data from csv. Skip for now: " + original_ticker)
        return
    

    #format: January 21, 2012
    added_date = datetime.strptime(added_date, "%B %d, %Y") 
    removal_date = (datetime.strptime(removal_date, "%B %d, %Y") if removal_date != None 
                    else datetime.strptime("September 30, 2024", "%B %d, %Y"))

    tiingo_market_cap_data = []
    fmp_market_cap_data = []

    #get market cap data from tiingo
    if get_tiingo_company_regular_data(ticker,company_name,{}):
        tiingo_market_cap_data = get_tiingo_market_cap_data(ticker, added_date)
    

    #get fmp market cap data if tiingo data is empty or starts after "Added Date"
    if len(tiingo_market_cap_data) == 0 or (datetime.strptime(tiingo_market_cap_data[0]["date"],"%Y-%m-%d") > added_date):
        if get_fmp_metadata(original_ticker,company_name,{}):
            fmp_market_cap_data = get_fmp_market_cap_data(original_ticker,index)

    tiingo_has_more_data = len(fmp_market_cap_data) <= len(tiingo_market_cap_data)
    if tiingo_has_more_data == False:
        print("Using fmp market data")

    market_cap_data = tiingo_market_cap_data if tiingo_has_more_data else fmp_market_cap_data

    #get only needed data in list
    market_cap_data = [data for data in market_cap_data
                        if added_date<= datetime.strptime(data["date"], "%Y-%m-%d") <= removal_date]
    
    if len(market_cap_data) > 0:
        #format: 2006-01-31
        first_date_in_data = datetime.strptime(market_cap_data[0]["date"], "%Y-%m-%d")
        last_date_in_data = datetime.strptime(market_cap_data[-1]["date"], "%Y-%m-%d")
        start_of_1996 = datetime.strptime("January 1, 1996", "%B %d, %Y")
        if added_date < first_date_in_data and start_of_1996 < first_date_in_data:
            days_between = 0
            if added_date > start_of_1996:
                days_between = (first_date_in_data - added_date).days
            else:
                days_between = (first_date_in_data - start_of_1996).days
            print("Missing days of earlier data for market cap: " + str(days_between))
        if last_date_in_data < removal_date:
            days_between = (removal_date - last_date_in_data).days
            print("Missing days of later data for market cap: " + str(days_between))

    if len(market_cap_data) < 500:
        if len(market_cap_data) == 0:
            print("No market cap data found: " + ticker)
        # else:
        #     print("Less than 2 years of market cap data: " + ticker)

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

for i, ticker in enumerate(tickers[:1157]):
    if i < 500:
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
        if ticker == "ASO":     ticker = "ASO1"
        if ticker == "EC":      ticker = "EC1"
        if ticker == "CHIR":    ticker = "CHIR1"
        if ticker == "BR":      ticker = "BR1"
        if ticker == "JP":      ticker = "JP1"
        if ticker == "DPH":     ticker = "DPHIQ"
        if ticker == "G":       ticker = "G1"
        if ticker == "SDS":     ticker = "SDS1"
        if ticker == "VRTS":    ticker = "VRTS1"
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
        if ticker == "SFS":     ticker = "SFS1"
        if ticker == "INGR":    ticker = "INGR1"

    #will get market cap data and place into file
    get_company_data(ticker, company_profile, company_name, meta_data_list, original_ticker, i)
    # get_market_cap_data(ticker, original_ticker, i, added_date, removal_date, company_name)




#NOTE: check 1048, RAD for market cap data; fmp has more data available and should be used if functioning correctly
    #for 1996-12-02, tiingo has $3.3 billion, while fmp has $5 billion
    #for 2001-06-28, tiingo has 3.5 billion, fmp has $4.2 billion, marketcap.com has $4.4 billion
    #for 2023-11-06, tiingo has $14.1 million, fmp has $13.7 million, marketcap.com has $36.8 million
#NOTE: check 1093, MTL/MBWM should use fmp data
#double check 977, GOLD for market cap calculation later