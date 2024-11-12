from dotenv import load_dotenv
import os
import requests
import fmpsdk


load_dotenv()
apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")


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
        
        is_right_company = False
        if tiingo_data["name"] != None:
            is_right_company = all([val in ["inc","co","corp","corporation","company","companies","the","-","&",
                                            "int`l","plc","ltd","llc",'(the)'] 
                                    or val in tiingo_data["name"].lower() or val in description.split()[:20]
                                    for val in company_name.split()])

        ticker_exception_list = ["DPZ","CPAY","CBOE","CB","ORLY","DOC","BXP","EL","LH","BF-B","GE","NSC","SLB"]
        #500s
        ticker_exception_list += ["XOM","FRCB","SIVBQ","DISCK","MRKT","DINO","AGN"]
        #600s
        ticker_exception_list += ["GGP","LVLT","DD","BBBYQ","MNKKQ","FTR","ENDPQ","TE","VAL","CNX","ALTR1"
                                  ,"CMCSA","PLL1","DTV1"]
        #700s
        ticker_exception_list += ["LIFE2","DELL1","FHN","DFODQ","BIGGQ","RRD","SUN1","ATGE","SHLDQ","MMI1","SUNEQ","NSM1"
                                  ,"Q1","PTV","MIL1","XTO","BDK","SGP","WINMQ"]
        #800s
        ticker_exception_list += ["WYE","CTX1","EQ1","ROH","SOV","UST1","AW","ABI1","BUD1","ASH","WWY","WEN","SAF2","FNMA","FMCC"
                                  ,"IAC","OMX1","CCTYQ","CBH1","CZR","DJ","AT1","BOL","AV1","TXU","ASN","SLR","CBSS","KSE"
                                  ,"BMET","MEL","PD1","PGL","APCC","EOP","SBL","BLS","NFB"]
        #900s
        ticker_exception_list += ["FSH","GTW","CTB","EC1","ABS","CHIR1","CIN","MYG","BR1","RBK","KRB","DPHIQ","G1","MAY"
                                  ,"NXTL","TOY","GLK","VRTS1","S1","WLP1","SOTR","AWE","ONE1","AM1","FBF","CE1","BIIB","QTRN"
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
            company_profile["exchange"] = tiingo_data["exchangeCode"] if tiingo_data["exchangeCode"] in ["NYSE", "NASDAQ"] else None
            if company_profile["description"] == tiingo_data["name"]:
                company_profile["description"] = None
            return True
        else:
            if ticker not in ["BK","BF-B","LLY","GE","IBM"]:
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
        company_profile["source"] = "tiingo"
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
            company_profile["source"] = "tiingo"
            match_found = True
            return True
    if match_found == False:
        print("No tiingo meta data found for: " + ticker)
        return False


def get_fmp_metadata(ticker, company_name, company_profile, index):
    fmp_bio_list = fmpsdk.company_profile(apikey=apikey, symbol=ticker)
    if (len(fmp_bio_list) > 0):
        fmp_data = fmp_bio_list[0]
        is_valid_exchange = fmp_data["exchangeShortName"] in ["NASDAQ", "NYSE"]
        company_name = company_name.replace('. ', ' ').replace('.', ' ') #do not remove commas, just "."
        is_right_company = False
        if fmp_data["companyName"] != None:
            is_right_company = all([val in ["inc","co","corp","corporation","company","companies","the","-","&",
                                            "int`l","plc","ltd","llc",'(the)'] 
                                    or val in fmp_data["companyName"].lower()
                                    for val in company_name.lower().split()])
            if is_right_company == False: 
                is_right_company = company_name.lower()[:8] in fmp_data["companyName"].lower().replace('. ', ' ').replace('.', ' ')

        has_exception = False
        if has_exception == False: has_exception = index<500 and ticker in ["BK","BF-B","LLY","GE","IMB"] #<500; unneeded
        if has_exception == False: has_exception = 500<=index<600 and ticker in ["XOM"] #500s; unneeded
        #nothing in 600s
        if has_exception == False: has_exception = 800<=index<900 and ticker in ["FNMA", "FMCC", "IAC"] #800s
        if has_exception == False: has_exception = 900<=index<1000 and ticker in ["SHEL"] #900s
        #nothing in 1000s, 1100s
        if has_exception or (is_valid_exchange and is_right_company):
            company_profile["company_name"] = fmp_data["companyName"]
            company_profile["sector"] = fmp_data["sector"]
            company_profile["industry"] = fmp_data["industry"]
            company_profile["is_delisted"] = not fmp_data["isActivelyTrading"]
            company_profile["description"] = fmp_data["description"]
            company_profile["source"] = "fmp"
            company_profile["exchange"] = fmp_data["exchangeShortName"] if fmp_data["exchangeShortName"] in ["NASDAQ", "NYSE"] else None


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
