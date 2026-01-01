import pandas as pd
from datetime import datetime
import requests
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

    for index, row in sp_500_data_ranges_df.iterrows():
        # if index < 10:
        #     continue
        print(index)
        company_profile = {}
        symbol = row['symbol']
        name = row['name']
        sector = row['sector']
        date_added = row['dateAdded']
        date_removed = row['dateRemoved']

        got_metadata = get_fmp_metadata(symbol, name, company_profile, index, part_2=True) #return True/False and update company_profile dict
        print(f"Got metadata for {symbol}: {got_metadata}")

        #write company profile to file
        profile_filename = f"part2/company_profiles/{index}_{symbol}.json"

        

        #get company profile data and add to folder in part2/company_profiles/


        #get company market cap data between date_added and date_removed and add to folder in part2/market_caps/

    # # get the company profiles dataframe
    # sp_500_company_profiles_df = get_company_profiles_df(sp_500_data_ranges_df, fmp_apikey)
    # print(sp_500_company_profiles_df)

    # # get the market caps dataframe
    # sp_500_market_caps_df = get_market_caps_df(sp_500_data_ranges_df, fmp_apikey)
    # print(sp_500_market_caps_df)


    # get_company_data(ticker, company_profile, company_name, meta_data_list, original_ticker, i)
    # get_market_cap_data(ticker, original_ticker, i, added_date, removal_date, company_name)