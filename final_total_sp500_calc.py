import pandas as pd
import matplotlib.pyplot as plt
import os

if __name__ == "__main__":
    # read sp500_index_data folder for yearly csv files and merge them into one csv file
    folder_path = "sp500_index_data/"
    df = pd.DataFrame()
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            yearly_df = pd.read_csv(file_path)
            df = pd.concat([df, yearly_df], ignore_index=True)
    # df.to_csv("final_data/merged_sp500_data.csv", index=False)

    # drop Open, High, Low columns
    df = df.drop(columns=["Open", "High", "Low"])
    # rename 'Close' to 'Closing Index Value'
    df = df.rename(columns={"Close": "Closing Index Value"})

    # get Date type (MM/DD/YYYY) to (YYYY-MM-DD)
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    # convert Closing Index Value to float
        #remove commas from Closing Index Value
    df['Closing Index Value'] = df['Closing Index Value'].str.replace(',', '')
    df['Closing Index Value'] = df['Closing Index Value'].astype(float)

    # sort by Date in ascending order
    df = df.sort_values(by="Date").reset_index(drop=True)

    # create value_multiple based on $58 trillion / Closing Index Value on 2025-12-31
    eoy_2025_market_cap = 5.8 * 10**13  # $58 trillion
    print(f"EOY 2025 Market Cap: {eoy_2025_market_cap}")
    eoy_index_2025 = df.loc[df['Date'] == '2025-12-31', 'Closing Index Value'].values[0]
    value_multiplier = eoy_2025_market_cap / eoy_index_2025
    print(f"Value Multiplier: {value_multiplier}")
    df['Total Market Capitalization'] = round(df['Closing Index Value'] * value_multiplier, 2)
    # print(df.head())

    # write to final_data/sp500_total_market_cap.csv
    df.to_csv("final_data/sp500_total_market_cap.csv", index=False)

    #graph Closing Index Value over time
    plt.figure(num=1, figsize=(12, 6))
    plt.plot(df['Date'], df['Closing Index Value'])
    plt.title('S&P 500 Closing Index Value Over Time')
    plt.xlabel('Date')
    plt.ylabel('Closing Index Value')
    plt.xticks(rotation=45)

    #remove x axis ticks except every 1 year
    plt.xticks(ticks=df.index[::252], labels=df['Date'][::252])
    plt.grid()
    # plt.show()
    plt.savefig("final_data/sp500_closing_index_value_over_time.png")


    #graph Market Capitalization over time
    plt.figure(num = 2, figsize=(12, 6))
    plt.plot(df['Date'], df['Total Market Capitalization'])
    plt.title('S&P 500 Total Market Capitalization Over Time')
    plt.xlabel('Date')
    plt.ylabel('Total Market Capitalization(USD in Trillions)')
    plt.xticks(rotation=45)

    #remove x axis ticks except every 1 year
    plt.xticks(ticks=df.index[::252], labels=df['Date'][::252])
    plt.grid()
    # modify y axis to show in trillions
    plt.gca().set_yticklabels([f'{y/1e12:.1f}' for y in plt.gca().get_yticks()])
    # plt.show()
    plt.savefig("final_data/sp500_total_market_cap_over_time.png")

