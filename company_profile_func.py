from dotenv import load_dotenv
import os
import requests

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
 

def get_fmp_metadata(ticker, company_name, company_profile, index, part_2=False):
    # https://financialmodelingprep.com/stable/profile?symbol=AAPL&apikey=4Wex9fEsJry4yFLT6k0mfNDxSKsHf726
    request = requests.get(f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={apikey}")
    fmp_bio_list = request.json()
    # fmp_bio_list = fmpsdk.company_profile(apikey=apikey, symbol=ticker)
    if (len(fmp_bio_list) > 0):
        fmp_data = fmp_bio_list[0]
        is_valid_exchange = fmp_data["exchange"] in ["NASDAQ", "NYSE"]
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
        if part_2 and has_exception == False: has_exception = ticker in ["SMCI","PCG","WAB","CBOE","IBM","SLB"]
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
            company_profile["exchange"] = fmp_data["exchange"] if fmp_data["exchange"] in ["NASDAQ", "NYSE"] else None

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

#will return the company profile metadata for edge cases or return None
def get_company_metadata_for_edge_cases(ticker, index):
    company_profiles = {
        ("INFO", 545): {
            "company_name": "IHS Markit Ltd","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Information Technology Services","exchange": "NYSE"},
        ("STI", 586): {
            "company_name": "SunTrust Banks Inc","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","source": "tiingo","exchange": "NYSE"},
        ("LLL", 595): {
            "company_name": "L3 Communications Holdings Inc","is_delisted": True,"description": None,
            "sector": "Industrials","industry": "Aerospace & Defense","source": "tiingo","exchange": "NYSE"},
        ("CA", 611): {
            "company_name": "CA Inc","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Software - Infrastructure","source": "tiingo","exchange": "NYSE"},
        ("XL", 612): {
            "company_name": "XL Group Ltd","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Insurance - Property & Casualty","exchange": "NYSE"},
        ("DPS", 614): {
            "company_name": "Dr Pepper Snapple Group Inc","is_delisted": True,"description": None,
            "sector": "Consumer Defensive","industry": "Beverages - Non-Alcoholic","exchange": "NYSE"},
        ("WYND", 620): {
            "company_name": "Wyndham Worldwide Corp","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Lodging","exchange": "NYSE"},
        ("DOW", 629): {
            "company_name": "Dow Chemical Company","is_delisted": True,"description": None,
            "sector": "Basic Materials","industry": "Chemicals","exchange": "NYSE"},
        ("HAR", 648): {
            "company_name": "Harman International Industries IncDE","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Consumer Electronics","exchange": "NYSE"},
        ("SE", 652): {
            "company_name": "Spectra Energy Corp","is_delisted": True,"description": None,
            "sector": "Utilities","industry": "Utilities - Regulated Gas","exchange": "NYSE"},
        ("EMC", 659): {
            "company_name": "EMC Corporation","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Hardware, Equipment & Parts","exchange": "NYSE"},
        ("FRX", 714): {
            "company_name": "Forest Laboratories Inc","is_delisted": True,"description": None,
            "sector": "Healthcare","industry": "Biotechnology","source": "tiingo","exchange": "NYSE"},
        ("LSI", 716): {
            "company_name": "LSI Corp","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Semiconductors","source": "tiingo","exchange": "NASDAQ"},
        ("BEAM", 718): {
            "company_name": "Beam Suntory Inc","is_delisted": True,"description": None,
            "sector": "Consumer Defensive","industry": "Beverages - Wineries & Distilleries","exchange": "NYSE"},
        ("JCP", 726): {
            "company_name": "JCPenney","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Department Stores","exchange": "NYSE"},
        ("NYX", 727): {
            "company_name": "NYSE Euronext","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Asset Management","source": "tiingo","exchange": "NYSE"},
        ("S", 732): {
            "company_name": "Sprint Corp","is_delisted": True,"description": None,
            "sector": "Communication Services","industry": "Telecom Services","source": "tiingo","exchange": "NYSE"},
        ("PGN", 750): {
            "company_name": "Progress Energy Inc.","is_delisted": True,"description": None,
            "sector": "Utilities","industry": "Utilities - Regulated Electric","source": "tiingo","exchange": "NYSE"},
        ("NVLS", 752): {
            "company_name": "Novellus Systems Inc","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Semiconductor Equipment & Materials","source": "tiingo","exchange": "NASDAQ"},
        ("EP", 753): {
            "company_name": "El Paso Corp","is_delisted": True,"description": None,
            "sector": "Energy","industry": "Oil & Gas E&P","source": "tiingo","exchange": "NYSE"},
        ("CEG", 757): {
            "company_name": "Constellation Energy Group Inc","is_delisted": True,"description": None,
            "sector": "Utilities","industry": "Utilities - Regulated Electric","source": "tiingo","exchange": "NYSE"},
        ("CPWR", 758): {
            "company_name": "Compuware Corporation","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Software - Services","exchange": "NASDAQ"},
        ("MI", 767): {
            "company_name": "Marshall & Ilsley Corp","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","source": "tiingo","exchange": "NYSE"},
        ("SII", 782): {
            "company_name": "Smith International","is_delisted": True,"description": None,
            "sector": "Energy","industry": "Oil & Gas Equipment & Services","exchange": "NYSE"},
        ("STR", 784): {
            "company_name": "Questar Corp","is_delisted": True,"description": None,
            "sector": "Energy","industry": "Oil & Gas E&P","source": "tiingo","exchange": "NYSE"},
        ("JAVA", 792): {
            "company_name": "Sun Microsystems Inc.","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Scientific & Technical Instruments","source": "tiingo","exchange": "NASDAQ"},
        ("DYN", 797): {
            "company_name": "Dynegy Inc.","is_delisted": True,"description": None,
            "sector": "Utilities","industry": "Utilities - Regulated Electric","source": "tiingo","exchange": "NYSE"},
        ("HPC", 828): {
            "company_name": "Hercules Inc.","is_delisted": True,"description": None,
            "sector": "Basic Materials","industry": "Specialty Chemicals","source": "tiingo","exchange": "NYSE"},
        ("EDS", 842): {
            "company_name": "Electronic Data Systems","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Information Technology Services","exchange": "NYSE"},
        ("TEK", 861): {
            "company_name": "Tektronix Inc.","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Semiconductor Equipment & Materials","source": "tiingo","exchange": "NYSE"},
        ("ADCT", 879): {
            "company_name": "ADC Telecommunications Inc.","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Communication Equipment","source": "tiingo","exchange": "NASDAQ"},
        ("MEDI", 880): {
            "company_name": "MedImmune Inc.","is_delisted": True,"description": None,
            "sector": "Healthcare","industry": "Biotechnology","source": "tiingo","exchange": "NASDAQ"},
        ("CMX", 885): {
            "company_name": "Caremark Rx Inc.","is_delisted": True,"description": None,
            "sector": "Consumer Defensive","industry": "Pharmaceutical Retailers","source": "tiingo","exchange": "NYSE"},
        ("LU", 897): {
            "company_name": "Lucent Technologies Inc.","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Communication Equipment","source": "tiingo","exchange": "NYSE"},
        ("MERQ", 924): {
            "company_name": "Mercury Interactive Corp","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Software - Application","source": "tiingo","exchange": "NASDAQ"},
        ("GP", 927): {
            "company_name": "Georgia-Pacific Corporation","is_delisted": True,"description": None,
            "sector": "Basic Materials","industry": "Paper & Paper Products","exchange": "NYSE"},
        ("PVN", 931): {
            "company_name": "Providian Financial Corporation","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","exchange": "NYSE"},
        ("PWER", 942): {
            "company_name": "Power-One, Inc","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Semiconductor Equipment & Materials","exchange": "NASDAQ"},
        ("PSFT", 943): {
            "company_name": "People Soft Inc","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Information Technology Services","exchange": "NASDAQ"},
        ("UPC", 952): {
            "company_name": "Union Planters Corp","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","source": "tiingo","exchange": "NYSE"},
        ("PCS", 956): {
            "company_name": "Sprint PCS Group","is_delisted": True,"description": None,
            "sector": "Communication Services","industry": "Telecom Services","exchange": "NYSE"},
        ("NSI", 995): {
            "company_name": "National Service Industries Inc","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Personal Services","source": "tiingo","exchange": "NYSE"},
        ("OAT", 1007): {
            "company_name": "Quaker Oats Co","is_delisted": True,"description": None,
            "sector": "Consumer Defensive","industry": "Packaged Foods","exchange": "NYSE"},
        ("H", 1010): {
            "company_name": "Harcourt General Inc.","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Publishing","exchange": "NYSE"},
        ("FPC", 1032): {
            "company_name": "Florida Progress Corp","is_delisted": True,"description": None,
            "sector": "Utilities","industry": "Utilities - Regulated Electric","source": "tiingo","exchange": "NYSE"},
        ("COMS", 1047): {
            "company_name": "3Com Corp.","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Communication Equipment","source": "tiingo","exchange": "NASDAQ"},
        ("USW", 1051): {
            "company_name": "US West Inc.","is_delisted": True,"description": None,
            "sector": "Communication Services","industry": "Telecommunications Services","exchange": "NYSE"},
        ("NLV", 1074): {
            "company_name": "NextLevel Systems Inc.","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Communication Equipment","exchange": "NYSE"},
        ("LI", 1077): {
            "company_name": "Laidlaw International, Inc.","is_delisted": True,"description": None,
            "sector": "Industrials","industry": "General Transportation","exchange": "NYSE"},
        ("TEN", 1083): {
            "company_name": "Tenneco Inc.","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Auto Parts","source": "tiingo","exchange": "NYSE"},
        ("AR", 1084): {
            "company_name": "Asarco Inc.","is_delisted": True,"description": None,
            "sector": "Basic Materials","industry": "Other Industrial Metals & Mining","source": "tiingo","exchange": "NYSE"},
        ("SNT", 1085): {
            "company_name": "Sonat Inc.","is_delisted": True,"description": None,
            "sector": "Energy","industry": "Oil & Gas Midstream","source": "tiingo","exchange": "NYSE"},
        ("BT", 1105): {
            "company_name": "Bankers Trust Corp.","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","source": "tiingo","exchange": "NYSE"},
        ("AN", 1117): {
            "company_name": "Amoco Corp.","is_delisted": True,"description": None,
            "sector": "Energy","industry": "Oil & Gas Refining & Marketing","source": "tiingo","exchange": "NYSE"},
        ("GRN", 1119): {
            "company_name": "General Reinsurance Corporation","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Reinsurance","exchange": "NYSE"},
        ("AS", 1120): {
            "company_name": "Armco Inc.","is_delisted": True,"description": None,
            "sector": "Basic Materials","industry": "Steel","source": "tiingo","exchange": "NYSE"},
        ("STO", 1121): {
            "company_name": "Stone Container Corp.","is_delisted": True,"description": None,
            "sector": "Basic Materials","industry": "Paper & Paper Products","exchange": "NYSE"},
        ("C", 1122): {
            "company_name": "Chrysler Corp","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Auto Manufacturers","source": "tiingo","exchange": "NYSE"},
        ("MCIC", 1130): {
            "company_name": "MCI Communications Corp","is_delisted": True,"description": None,
            "sector": "Communication Services","industry": "Telecommunications Services","exchange": "NASDAQ"},
        ("MST", 1133): {
            "company_name": "Mercantile Stores Inc","is_delisted": True,"description": None,
            "sector": "Consumer Cyclical","industry": "Department Stores","exchange": "NYSE"},
        ("WAI", 1134): {
            "company_name": "Western Atlas Inc.","is_delisted": True,"description": None,
            "sector": "Energy","industry": "Oil & Gas Equipment & Services","source": "tiingo","exchange": "NYSE"},
        ("BNL", 1138): {
            "company_name": "Beneficial Corp","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Credit Services","source": "tiingo","exchange": "NYSE"},
        ("GNT", 1140): {
            "company_name": "Green Tree Financial Corp","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Mortgage Finance","exchange": "NYSE"},
        ("PET", 1141): {
            "company_name": "Pacific Enterprises","is_delisted": True,"description": None,
            "sector": "Utilities","industry": "Utilities - Regulated Gas","source": "tiingo","exchange": "NYSE"},
        ("DEC", 1142): {
            "company_name": "Digital Equipment Corp","is_delisted": True,"description": None,
            "sector": "Technology","industry": "Information Technology Services","exchange": "NYSE"},
        ("CFL", 1143): {
            "company_name": "Corestates Financial Corp","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","source": "tiingo","exchange": "NYSE"},
        ("FG", 1144): {
            "company_name": "USF&G Corp","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Insurance - Life","exchange": "NYSE"},
        ("CBB", 1150): {
            "company_name": "Caliber System Inc.","is_delisted": True,"description": None,
            "sector": "Industrials","industry": "Integrated Freight & Logistics","exchange": "NYSE"},
        ("BBI", 1152): {
            "company_name": "Barnett Banks Inc.","is_delisted": True,"description": None,
            "sector": "Financial Services","industry": "Banks - Regional","exchange": "NYSE"}
    }
    key = (ticker, index)
    return company_profiles.get(key)






#will return the stock exchange(NYSE or NASDAQ) for edge cases or return None
def get_stock_exchange_for_edge_cases(index):
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
    return exchange_dict.get(index)