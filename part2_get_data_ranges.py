import pandas as pd
from datetime import datetime
import requests
import os
from dotenv import load_dotenv

load_dotenv()

fmp_apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")

if __name__ == "__main__":
    # current_sp_list = fmpsdk.sp500_constituent(apikey=fmp_apikey)

    # print(f"Total number of S&P 500 companies: {len(current_sp_list)}")
    # print(current_sp_list)

    # https://financialmodelingprep.com/stable/sp500-constituent?apikey=4Wex9fEsJry4yFLT6k0mfNDxSKsHf726
    request = requests.get(f"https://financialmodelingprep.com/stable/sp500-constituent?apikey={fmp_apikey}")
    current_sp_list = request.json() # is a list of dicts/json objects
    # only need the 'symbol', 'name', 'sector', 'dateAdded' fields
    current_sp_list = [{ 'symbol': record['symbol'], 'name': record['name'], 'sector': record['sector'], 
                        'dateAdded': record['dateFirstAdded']} for record in current_sp_list]
    

    # print(current_sp_list)


    #convert list of dicts to dataframe
    sp_500_df = pd.DataFrame(current_sp_list)
    # add empty dateRemoved column; is "2026-01-01" for current constituents
    sp_500_df['dateRemoved'] = '2026-01-01'
    # print(sp_500_df)
    
    
    
    
    request = requests.get(f"https://financialmodelingprep.com/stable/historical-sp500-constituent?apikey={fmp_apikey}")
    historical_sp_list = request.json() # is a list of dicts/json objects
    full_historical_sp_list = historical_sp_list.copy()
    # print(len(historical_sp_list))
    # print(historical_sp_list) 
       #list of dicts :  {'dateAdded': 'November 12, 1993', 'addedSecurity': 'Barrick Gold Corp.', 'removedTicker': 'AMX', 'removedSecurity': 'AMAX Inc.', 'date': '1993-11-12', 'symbol': 'ABX', 'reason': 'Annual Re-ranking'}

    # list goes from most recent to oldest; need to remove json objects that are past Sept. 30, 2024
    temp_list = []
    date_ranges = []
    end_date = None
    start_date = None
    for record in historical_sp_list:
        if record['date'] > '2026-01-01': #skip entries beyond 2025
            continue
        if record['date'] >= '2024-09-30':
            if start_date == None or record['date'] < start_date:
                # print("Checking date:", record['date'])
                # print("Previous start_date:", start_date)
                #end date is the previous start date
                end_date = start_date if start_date is not None else '2026-01-01'
                if end_date != "2026-01-01": # decrease end date by one day
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    end_date_dt = end_date_dt.replace(day=end_date_dt.day - 1)
                    end_date = end_date_dt.strftime('%Y-%m-%d')
                start_date = record['date']
                date_ranges.append((record['date'], end_date))
            temp_list.append(record)
        else:
            break
    historical_sp_list = temp_list
    # print(historical_sp_list)   

    # for the historica list, we need to add the removed ticker/company to the bottom of the main dataframe
    for record in historical_sp_list:
        symbol = record['removedTicker']
        name = record['removedSecurity']
        # dataAdded is None for now
        date_removed = record['date']
        # add a new row to the dataframe for the removed ticker/company
        sp_500_df = pd.concat([sp_500_df, pd.DataFrame([{
            'symbol': symbol,
            'name': name,
            'sector': None,
            'dateAdded': None,
            'dateRemoved': date_removed
        }])], ignore_index=True)

    dict_ticker_sectors = {'SOLS': 'Financial Services', 'LKQ': 'Consumer Cyclical', 'MHK': 'Consumer Cyclical', 'K': 'Consumer Defensive',
            'IPG': 'Communication Services', 'EMN': 'Basic Materials', 'KMX': 'Consumer Cyclical', 
            'CZR': 'Consumer Cyclical', 'ENPH': 'Energy', 'MKTX': 'Financial Services', 'WBA': 'Healthcare',
            'HES': 'Energy', 'ANSS': 'Technology', 'JNPR': 'Technology', 'DFS': 'Financial Services', 
            'FMC': 'Basic Materials', 'CE': 'Basic Materials', 'TFX': 'Healthcare', 'BWA': 'Consumer Cyclical', 
            'QRVO': 'Technology', 'AMTM': 'Industrials', 'CTLT': 'Healthcare', 'MRO': 'Energy', 'BBWI': 'Consumer Cyclical'}

    # fix rows with missing dateAdded values or sector values in the last 30 rows of df
    for index, row in sp_500_df.tail(30).iterrows():
        if pd.isnull(row['dateAdded']) or row['dateAdded'] is None:
            if row['symbol'] == "SOLS":
                sp_500_df.at[index, 'dateAdded'] = '2025-10-31' # was under different ticker ,SOLSV
            else:
            # look up dateAdded from historical list by now checked full list for when it was added
                for record in full_historical_sp_list:
                    if record['symbol'] == row['symbol']:
                        # need to convert dateAdded from "November 12, 1993" to "11/12/1993"
                        date_added = record['dateAdded']
                        month, day, year = date_added.split(' ')
                        day = day.replace(',', '')
                        day = '0' + day if len(day) == 1 else day
                        month_number = datetime.strptime(month, '%B').month
                        sp_500_df.at[index, 'dateAdded'] = f"{year}-{month_number}-{day}"
                        break
        if pd.isnull(row['sector']) or row['sector'] is None:
            # look up sector from request: https://financialmodelingprep.com/stable/profile?symbol=LKQ&apikey=4Wex9fEsJry4yFLT6k0mfNDxSKsHf726
            symbol = row['symbol']
            if symbol in dict_ticker_sectors:
                sector = dict_ticker_sectors[symbol]
                sp_500_df.at[index, 'sector'] = sector
    #         profile_request = requests.get(f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={fmp_apikey}")
    #         profile_data = profile_request.json()
    #         # print("symbol:", symbol, "number of profile_data:", len(profile_data))
    #         if len(profile_data) > 0:
    #             sector = profile_data[0].get('sector', None) # get sector from profile data
    #             sp_500_df.at[index, 'sector'] = sector

    #         # make list for future reference
    #         dict_ticker_sectors[symbol] = sector
    # print(dict_ticker_sectors)

    #write to csv
    #make new directory for "part2_data" if it doesn't exist
    if not os.path.exists("part2_data"):
        os.makedirs("part2_data")
    # sp_500_df.to_csv("part2_data/sp_500_dataset.csv", index=False)

    # write date ranges to a csv file with columns start_date and end_date
    date_ranges_df = pd.DataFrame(date_ranges, columns=['date_range_start', 'date_range_end'])
    date_ranges_df.to_csv("part2_data/sp_500_date_ranges.csv", index=False)