import pandas as pd
from datetime import datetime
import requests
import json
import os
from company_profile_func import *
from market_cap_func import * 
from dotenv import load_dotenv


load_dotenv()

fmp_apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")



if __name__ == "__main__":
    # get the data ranges dataframe
    sp_500_data_ranges_df = pd.read_csv("part2_data/sp_500_dataset.csv")


    if not os.path.exists("part2_data/company_profiles/"):
        os.makedirs("part2_data/company_profiles/")
    if not os.path.exists("part2_data/company_market_cap_data/"):
        os.makedirs("part2_data/company_market_cap_data/")
    if not os.path.exists("part2_data/market_cap_metadata/"):
        os.makedirs("part2_data/market_cap_metadata/")

    for index, row in sp_500_data_ranges_df.iterrows():
        if index < 500:
            continue
        print(index)
        company_profile = {}
        symbol = row['symbol']
        name = row['name']
        sector = row['sector']
        date_added = row['dateAdded']
        date_removed = row['dateRemoved']

        # #get company profile data and add to folder in part2/company_profiles/
        # got_metadata = get_fmp_metadata(symbol, name, company_profile, index, part_2=True) #return True/False and update company_profile dict
        # # print(f"Got metadata for {symbol}: {got_metadata}")
        # #write company profile to file
        # profile_filename = f"part2_data/company_profiles/{index}_{symbol}.json"
        # with open(profile_filename, 'w') as f:
        #     json.dump(company_profile, f, indent=4)

        
        # get company market cap data between date_added and date_removed and add to folder in part2/market_cap_data/
        start_date = date_added
        end_date = date_removed 
        #if start_date is before 2024-10-01, set to 2024-10-01
        if datetime.strptime(start_date, '%Y-%m-%d') < datetime.strptime('10/01/2024', '%m/%d/%Y'):
            start_date = '2024-10-01'
        market_cap_data = part_2_get_fmp_market_cap_data(symbol, start_date, end_date)
        if market_cap_data == []:
            print(f"No market cap data for {symbol}")
            continue
        # print(market_cap_data)
        # save market cap data to file and overwrite if exists
        market_cap_filename = f"part2_data/company_market_cap_data/{index}_{symbol}.json"
        with open(market_cap_filename, 'w') as f:
            json.dump(market_cap_data, f, indent=4)


        meta_data = {
            "index": index,
            "ticker": symbol,
            "source": "fmp",
            "first_day_have_vs_needed": f"{market_cap_data[0]['date']} : {start_date}",
            "last_day_have_vs_needed": f"{market_cap_data[-1]['date']} : {end_date}",
            "num_of_days_data": len(market_cap_data)
        }
        with open(f"part2_data/market_cap_metadata/{index}_{symbol}.json", 'w') as f:
            json.dump(meta_data, f, indent=4)
