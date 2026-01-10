import pandas as pd
from datetime import datetime
import requests
import os
from dotenv import load_dotenv

load_dotenv()

fmp_apikey = os.environ.get("fmp_apikey")
tiingo_token = os.environ.get("tiingo_token")

if __name__ == "__main__":
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
                    data = f.read()
                with open(os.path.join("final_data/market_cap_metadata/", new_filename), 'w') as f:
                    f.write(data)
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
            part1_data = f.read()
        with open(os.path.join("part2_data/company_market_cap_data/", part2_file), 'r') as f:
            part2_data = f.read()


        # get metadata source from both files
        part1_data_source, part2_data_source = None, None
        with open(os.path.join("part1_data/market_cap_metadata/", part1_file), 'r') as f:
            part1_metadata = f.read()
            part1_data_source = eval(part1_metadata).get('source', None)
        with open(os.path.join("part2_data/market_cap_metadata/", part2_file), 'r') as f:
            part2_metadata = f.read()
            part2_data_source = eval(part2_metadata).get('source', None)

        data_source = None
        if part1_data_source == part2_data_source:
            data_source = part1_data_source #same source; likely "fmp"
        else:   # different sources; store both in json
            data_source = {"part1": part1_data_source, "part2": part2_data_source}

 
        #combine the two json strings into one list of dicts/json objects
        combined_data = []
        combined_data.extend(eval(part1_data))
        combined_data.extend(eval(part2_data))
        #write combined data to final folder
        new_filename = f"{final_index}_{symbol}.json"
        with open(os.path.join("final_data/company_market_cap_data/", new_filename), 'w') as f:
            f.write(str(combined_data))

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
            f.write(str(metadata))

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
                data = f.read()
                with open(os.path.join("final_data/company_market_cap_data/", final_filename), 'w') as f:
                    f.write(data)
            # copy company profile file
            with open(os.path.join("part1_data/company_profiles/", file_name), 'r') as f:
                data = f.read()
                with open(os.path.join("final_data/company_profiles/", final_filename), 'w') as f:
                    f.write(data)
            # copy market cap metadata file
            with open(os.path.join("part1_data/market_cap_metadata/", file_name), 'r') as f:
                data = f.read()
                with open(os.path.join("final_data/market_cap_metadata/", final_filename), 'w') as f:
                    f.write(data)
            final_index += 1


    
    print(f"Finished going through part1 files. Final index is now {final_index}.")       