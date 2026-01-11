import pandas as pd
from datetime import datetime
import requests
import calendar
import json
import os
from dotenv import load_dotenv

load_dotenv()

fmp_apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")



def get_raw_date_ranges_test():
    request = requests.get(f"https://financialmodelingprep.com/stable/historical-sp500-constituent?apikey={fmp_apikey}")
    historical_sp_list = request.json() # is a list of dicts/json objects

    end_date, start_date = None, None
    date_ranges = []
    date_ranges_dict = {} # key is start_date : end_date, value is number of constituents
    for record in historical_sp_list:
        date = record['date'] #should match a date_range_start in full_date_ranges_df
        if date > '2025-12-31':
            continue
        if date < '1998-01-01':
            break  #we only need up to 1998-01-01


        if start_date is None or date < start_date:
            end_date = start_date if start_date is not None else '2026-01-01'
            start_date = date
            if end_date != "2026-01-01": # decrease end date by one day
                end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
                #need to handle end of month cases
                if end_date_dt.day == 1:
                    # print("Date with day 1 found:", end_date)
                    new_month = end_date_dt.month - 1
                    year = end_date_dt.year
                    if new_month == 0:
                        new_month = 12
                        year -= 1
                    num_days = calendar.monthrange(year, new_month)[1]
                    # last_day = datetime.date(year, new_month, num_days)
                    # create last day datetime
                    last_day = end_date_dt.replace(month=new_month, day=num_days) 
                    end_date_dt = last_day
                else:
                    end_date_dt = end_date_dt.replace(day=end_date_dt.day - 1)
                end_date = end_date_dt.strftime('%Y-%m-%d')
            date_ranges.append({
                'date_range_start': start_date,
                'date_range_end': end_date
            })

    raw_date_ranges_df = pd.DataFrame(date_ranges)
    # raw_date_ranges_df.to_csv("final_data/SP500_raw_date_ranges.csv", index=False)
    return raw_date_ranges_df



def update_date_ranges_with_constituents(full_date_ranges_df):
    # request = requests.get(f"https://financialmodelingprep.com/stable/sp500-constituent?apikey={fmp_apikey}")
    # current_sp500_constituents = request.json()  # is a list of dicts/json objects
    # # just get symbols from current_sp500_constituents
    # current_sp500_constituents = [record['symbol'] for record in current_sp500_constituents]

    # #write current_sp500_constituents to a text file in final_data/date_range_constituents/ named 2025-12-31_constituents.txt
    # with open(f"final_data/2025-12-31_constituents.txt", "w") as f:
    #     for symbol in current_sp500_constituents:
    #         f.write(f"{symbol}\n")

    current_sp500_constituents = []
    # get current SP500 constituents from 2025-12-31_constituents.txt
    with open(f"final_data/2025-12-31_constituents.txt", "r") as f:
        for line in f:
            current_sp500_constituents.append(line.strip())

    #create a folder final_data/date_range_constituents/ if it doesn't exist
    if not os.path.exists("final_data/date_range_constituents/"):
        os.makedirs("final_data/date_range_constituents/")


    number_of_sp500_constituents = 503  #as of end of 2025
    full_date_ranges_df['number_of_constituents'] = None
    # for first date range, should be 503
    full_date_ranges_df.at[0, 'number_of_constituents'] = 503
    # for subsequent date ranges, need to get historical constituents
    request = requests.get(f"https://financialmodelingprep.com/stable/historical-sp500-constituent?apikey={fmp_apikey}")
    historical_sp_list = request.json() # is a list of dicts/json objects
    for record in historical_sp_list:
        date = record['date'] #should match a date_range_start in full_date_ranges_df
        if date > '2025-12-31': #skip entries beyond 2025
            continue
        if date < '1998-01-01':
            break  #we only need up to 1998-01-01
        # check if date in full_date_ranges_df has None for number_of_constituents

        # if date == "2024-04-01": date = "2024-04-02"  #fix for known date issue from FMP

        index_match = full_date_ranges_df[full_date_ranges_df['date_range_start'] == date].index
        if len(index_match) > 0:
            index = index_match[0]

            # if number_of_sp500_constituents is None, set to current number
            if full_date_ranges_df.at[index, 'number_of_constituents'] is None:
                full_date_ranges_df.at[index, 'number_of_constituents'] = number_of_sp500_constituents

            is_removing_security = record['removedTicker'] is not None and record['removedTicker'] != ''
            is_adding_security = record['addedSecurity'] is not None and record['addedSecurity'] != ''

            #NOTE: working backwards in time, so adding security means removing from current list, and vice versa
            # update to current number_of_sp500_constituents if empty
            if full_date_ranges_df.at[index, 'number_of_constituents'] is None: 
                full_date_ranges_df.at[index, 'number_of_constituents'] = number_of_sp500_constituents
            # if both adding and removing securities, adjust accordingly
            if is_removing_security and is_adding_security:
                current_sp500_constituents.append(record['removedTicker'])
                if record['symbol'] in current_sp500_constituents:
                    current_sp500_constituents.remove(record['symbol'])
                # exceptions to handle" SOLSV , GEN, FI
                elif record['symbol'] == "SOLSV":
                        current_sp500_constituents.remove("SOLS")
                # two tickers with GEN, different exchanges; unsure if actually part of SP500
                elif record['symbol'] == "GEN":
                    print(f"Issue with GEN ticker on date {date}, skipping removal.")
                    number_of_sp500_constituents += 1 # temporarily adjust count

                # FI -> FISV
                elif record['symbol'] == "FI":
                    current_sp500_constituents.remove("FISV")
                else:
                    print(f"1_Warning: Ticker {record['symbol']} to be removed on date {date} not found in current constituents.")
                number_of_sp500_constituents += 0
            elif is_removing_security:
                current_sp500_constituents.append(record['removedTicker'])
                # print(f"Just removing security on date {date}")
                #check if removedTicker is in current_sp500_constituents
                # if record['removedTicker'] in current_sp500_constituents:
                #     current_sp500_constituents.remove(record['removedTicker'])
                number_of_sp500_constituents += 1
            elif is_adding_security:
                # print(f"Just adding security on date {date}")
                if record['symbol'] in current_sp500_constituents:
                    current_sp500_constituents.remove(record['symbol'])
                else:
                    print(f"2_Warning: Ticker {record['addedSecurity']} to be removed on date {date} not found in current constituents.")
                
                number_of_sp500_constituents -= 1

            full_date_ranges_df.at[index, 'number_of_constituents'] = number_of_sp500_constituents

            #save current_sp500_constitutents to a text file in final_data/date_range_constituents/ named {date}_constituents.txt
            with open(f"final_data/date_range_constituents/{date}_constituents.txt", "w") as f:
                #sort current_sp500_constituents alphabetically
                current_sp500_constituents.sort()
                f.write("\n".join(current_sp500_constituents))

            # #check if number_of_constituents is not 503
            # if number_of_sp500_constituents != 503:
            #     print(f"Date: {date}, Number of constituents: {number_of_sp500_constituents}")
            # check if number_of_constituents matches length of current_sp500_constituents
            if number_of_sp500_constituents != len(current_sp500_constituents):
                print(f"3_Warning: Date: {date}, Number of constituents: {number_of_sp500_constituents}, Length of current constituents list: {len(current_sp500_constituents)}")

        else:
            print (f"Date {date} not found in date ranges.")
    
    #Add 1998-01-01 to 1998-01-08 with last known number_of_constituents
    full_date_ranges_df = pd.concat([full_date_ranges_df, pd.DataFrame([{
                                    'date_range_start': '1998-01-01',
                                    'date_range_end': '1998-01-08',
                                    'number_of_constituents': number_of_sp500_constituents
                                }])], ignore_index=True)
    # update data_ranges file with number_of_constituents
    full_date_ranges_df.to_csv("final_data/SP500_date_ranges.csv", index=False)

































        # check if date in full_date_ranges_df has None for number_of_constituents
if __name__ == "__main__":
    # make final_data/ folder if it doesn't exist
    if not os.path.exists("final_data/"):
        os.makedirs("final_data/")

    # full_date_ranges_df = get_raw_date_ranges_test()  #create raw date ranges file
    # update_date_ranges_with_constituents(full_date_ranges_df)  #update date ranges with number of constituents

 
    # # stop execution here for now
    # if True:
    #     exit()



    #part2_data/sp_500_dataset.csv
    sp_500_df = pd.read_csv("part2_data/sp_500_dataset.csv")

    request = requests.get(f"https://financialmodelingprep.com/stable/historical-sp500-constituent?apikey={fmp_apikey}")
    historical_sp_list = request.json() # is a list of dicts/json objects
    # full_historical_sp_list = historical_sp_list.copy()

    #from historical list, we need to get the tickers that were added after Sept. 30, 2024
    stock_list = []
    for record in historical_sp_list:
        if record['date'] > '2024-09-30':
            stock_list.append(record)
        else:
            break
    #add SOLS to the list; was under a different ticker before, SOLSV
    stock_list.append({
        'dateAdded': 'October 31, 2024',
        'addedSecurity': 'Solstice Advanced Materials Inc.',
        'removedTicker': None,
        'removedSecurity': None,
        'date': '2024-10-31',
        'symbol': 'SOLS',
        'reason': 'IPO'
    })

    # from sp_500_df, we need to remove any tickers that are in stock_list
    previous_sp_500_df = sp_500_df.copy()
    print(f"Number of stocks before removal: {len(sp_500_df)}")
    print(f"Number of stocks to remove: {len(stock_list)}")
    for record in stock_list:
        symbol = record['symbol']
        previous_sp_500_df = previous_sp_500_df[previous_sp_500_df['symbol'] != symbol]

    print(f"Number of stocks after removal: {len(previous_sp_500_df)}")


    # we now have the SP500 dataset at Sept. 30, 2024

    # print([(stock['symbol'], stock['addedSecurity']) for stock in stock_list])


    # NOTES:
        #24 stocks were added after Sept. 30, 2024
        #25 were also removed from the main SP500 dataset


    #task: combine part1 and part2 datasets
        #go through list_of_new_stocks, previous_sp_500_df, and then part1_data/cleaned_sp_500_dataset.csv
        #add to "final" folders: final_data/company_profiles/ and final_data/company_market_cap_data/ and final_data/market_cap_metadata/
    if not os.path.exists("final_data/company_profiles/"):
        os.makedirs("final_data/company_profiles/")
    if not os.path.exists("final_data/company_market_cap_data/"):
        os.makedirs("final_data/company_market_cap_data/")
    if not os.path.exists("final_data/market_cap_metadata/"):
        os.makedirs("final_data/market_cap_metadata/")

    final_index = 0

    print("Going through stocks added to SP500 after Sept. 30, 2024...")
    for record in stock_list:
        # copy company profile file from part2_data/company_profiles/ to final/company_profiles/
        # copy market cap data file from part2_data/company_market_cap_data/ to final/company_market_cap_data/
        # copy market cap metadata file from part2_data/market_cap_metadata/ to final/market_cap_metadata/
        #search folder for file with same symbol
        symbol = record['symbol']
        for filename in os.listdir("part2_data/company_profiles/"):
            if filename.endswith(".json") and f"_{symbol}." in filename:
                #copy file to final folder with new index
                new_filename = f"{final_index}_{symbol}.json"
                with open(os.path.join("part2_data/company_profiles/", filename), 'r') as f:
                    data = f.read()
                with open(os.path.join("final_data/company_profiles/", new_filename), 'w') as f:
                    f.write(data)
                break
        for filename in os.listdir("part2_data/company_market_cap_data/"):
            if filename.endswith(".json") and f"_{symbol}." in filename:
                #copy file to final folder with new index
                new_filename = f"{final_index}_{symbol}.json"
                with open(os.path.join("part2_data/company_market_cap_data/", filename), 'r') as f:
                    data = f.read()
                with open(os.path.join("final_data/company_market_cap_data/", new_filename), 'w') as f:
                    f.write(data)
                break
        for filename in os.listdir("part2_data/market_cap_metadata/"):
            if filename.endswith(".json") and f"_{symbol}." in filename:
                #copy file to final folder with new index
                new_filename = f"{final_index}_{symbol}.json"
                with open(os.path.join("part2_data/market_cap_metadata/", filename), 'r') as f:
                    data = json.load(f)
                with open(os.path.join("final_data/market_cap_metadata/", new_filename), 'w') as f:
                    json.dump(data, f, indent=4)
                break
        final_index += 1

    print(f"Finished going through stocks added to SP500 after Sept. 30, 2024. Final index is now {final_index}.")

    # go through previous_sp_500_df and part1_data/cleaned_sp_500_dataset.csv
        # create a list of referenced part1 files so we don't have duplicates later
            #either have everything then delete or keep track as we go
    list_of_referenced_part1_files = []
    
    print("Going through SP500 stocks on Sept. 30, 2024...")
    file1_index = 0
    for file2_index, row in previous_sp_500_df.iterrows():
        # print(file1_index)
        # copy company profile file from part2_data/company_profiles/ to final/company_profiles/
        # combine market cap data file from part2_data/company_market_cap_data/ and part1_data/company_market_cap_data/ to final/company_market_cap_data/

        symbol = row['symbol']

        
        part2_file = f"{file2_index}_{symbol}.json"
        part1_file = f"{file1_index}_{symbol}.json"



     


        #check if files exist for part2
        if not os.path.exists(os.path.join("part2_data/company_market_cap_data/", part2_file)):
            print(f"Part2 market cap data file not found for {symbol}: {part2_file}")
            # continue

        dict_part1_location = {"FANG": 95, "LKQ": 143, "MHK": 179, "K": 381, "IPG": 376, "EMN": 372, "KMX": 218, 
                        "CZR": 52,"ENPH": 57, "MKTX": 84, "WBA": 421, "HES": 403, "ANSS": 119
                        ,"JNPR": 269, "DFS": 259, "FMC": 226, "CE": 94, "TFX": 93, "BWA": 202 
                        ,"QRVO": 164, "CTLT": 60, "MRO": 486, "BBWI": 408} 
        list_part1_used_indexes = [] # to avoid duplicates in last part


        # check if files exist for part1
        if not os.path.exists(os.path.join("part1_data/company_market_cap_data/", part1_file)):
            # check if moving file1_index forward by 1 helps
            if os.path.exists(os.path.join("part1_data/company_market_cap_data/", f"{file1_index + 1}_{symbol}.json")):
                file1_index += 1
                part1_file = f"{file1_index}_{symbol}.json"
            # check if moving file1_index forward by 2 helps
            elif os.path.exists(os.path.join("part1_data/company_market_cap_data/", f"{file1_index + 2}_{symbol}.json")):
                file1_index += 2
                part1_file = f"{file1_index}_{symbol}.json"
            elif symbol == "FISV": #311 
                part1_file = "311_FI.json"
                # look for "FI" ticker instead: "part1_data\company_market_cap_data\311_FI.json"
            elif symbol == "PSKY": #366
                part1_file = "366_PARA.json"
                # look for "PARA" ticker instead: "part1_data\company_market_cap_data\366_PARA.json"
            elif symbol in dict_part1_location:
                part1_file = f"{dict_part1_location[symbol]}_{symbol}.json"
            else:
                print(f"Part1 market cap data file not found for {symbol}: {part1_file}")


        # add the part1 file index to list_of_referenced_part1_files
        if symbol == "FISV": 
            # print("at FISV")
            list_of_referenced_part1_files.append(311)
        elif symbol == "PSKY": 
            # print("at PSKY")
            list_of_referenced_part1_files.append(366)
        elif symbol in dict_part1_location:
            # print(f"at {symbol} with different index")
            list_of_referenced_part1_files.append(dict_part1_location[symbol])
        else:
            list_of_referenced_part1_files.append(file1_index)

        # combine market cap data files, part1 + part2, both are json files with a list of dicts/json objects
        part1_data, part2_data = None, None
        with open(os.path.join("part1_data/company_market_cap_data/", part1_file), 'r') as f:
            part1_data = json.load(f)
        with open(os.path.join("part2_data/company_market_cap_data/", part2_file), 'r') as f:
            part2_data = json.load(f)


        # get metadata source from both files
        part1_data_source, part2_data_source = None, None
        with open(os.path.join("part1_data/market_cap_metadata/", part1_file), 'r') as f:
            part1_metadata = json.load(f)
            part1_data_source = part1_metadata.get('source', None)
        with open(os.path.join("part2_data/market_cap_metadata/", part2_file), 'r') as f:
            part2_metadata = json.load(f)
            part2_data_source = part2_metadata.get('source', None)

        data_source = None
        if part1_data_source == part2_data_source:
            data_source = part1_data_source #same source; likely "fmp"
        else:   # different sources; store both in json
            data_source = {"part1": part1_data_source, "part2": part2_data_source}

 
        #combine the two json strings into one list of dicts/json objects
        combined_data = []
        combined_data = part1_data + part2_data
        # print(type(combined_data))
        # print(combined_data[0])
        # print(combined_data)

        #write combined data to final folder as json file
        new_filename = f"{final_index}_{symbol}.json"
        with open(os.path.join("final_data/company_market_cap_data/", new_filename), 'w') as f:
            # need to replace single quotes with double quotes for valid json
            # f.write(str(combined_data).replace("'", '"'))
            # write as json

            json.dump(combined_data, f, indent=4)

        with open(os.path.join("part2_data/company_profiles/", f"{file2_index}_{symbol}.json"), 'r') as f:
            profile_data = f.read()
            with open(os.path.join("final_data/company_profiles/", new_filename), 'w') as f:
                f.write(profile_data)

        with open(os.path.join("final_data/market_cap_metadata/", new_filename), 'w') as f:
            metadata = {
                "index": final_index,
                "ticker": symbol,
                "source_part1_file_index": file1_index,
                "source_part2_file_index": file2_index,
                "source": data_source,
                "first_day_of_data": combined_data[0]['date'] if combined_data else None,
                "last_day_of_data": combined_data[-1]['date'] if combined_data else None,
                "number of trading days": len(combined_data)
            }
            json.dump(metadata, f, indent=4)

        final_index += 1
        file1_index += 1

    print(f"Finished going through SP500 stocks on Sept. 30, 2024. Final index is now {final_index}.")


    # # after list_of_referenced_part1_files is made, sort it in alphabetical/numerical order by index
    list_of_referenced_part1_files.sort()

    # print(f"Number of referenced part1 files: {len(list_of_referenced_part1_files)}")
    # print("List of referenced part1 files:", list_of_referenced_part1_files)





    print("Going through part1 files to add remaining companies...")

    #go through part1 files, and if index not in list_of_referenced_part1_files, copy to final folders with new index
        # files are named {index}_{symbol}.json for each of the folders in part1_data/
    part1_file_list = os.listdir("part1_data/company_market_cap_data/")
    part1_file_list.sort(key=lambda x: int(x.split('_')[0])) # sort part1_file_list by index number
    for file_name in part1_file_list:
        index_number = int(file_name.split('_')[0])
        if index_number > 1152:
            break #anything past this index is not relevant, being removed before 1998
        # print(file_name)
        if index_number not in list_of_referenced_part1_files:
            # print(file_name)
            symbol = file_name.split('_')[1].split('.')[0]
            final_filename = f"{final_index}_{symbol}.json"
            # copy market cap data file
            with open(os.path.join("part1_data/company_market_cap_data/", file_name), 'r') as f:
                data = json.load(f)
                with open(os.path.join("final_data/company_market_cap_data/", final_filename), 'w') as f:
                    json.dump(data, f, indent=4)
            # copy company profile file
            with open(os.path.join("part1_data/company_profiles/", file_name), 'r') as f:
                data = json.load(f)
                with open(os.path.join("final_data/company_profiles/", final_filename), 'w') as f:
                    json.dump(data, f, indent=4)
            # copy market cap metadata file
            with open(os.path.join("part1_data/market_cap_metadata/", file_name), 'r') as f:
                data = json.load(f)
                with open(os.path.join("final_data/market_cap_metadata/", final_filename), 'w') as f:
                    json.dump(data, f, indent=4)
            final_index += 1


    print(f"Finished going through part1 files. Final index is now {final_index}.")