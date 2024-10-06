import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd

load_dotenv()
apikey = os.environ.get("apikey")

def get_sp_500_github_dataset():
    #this csv comes from this 3rd party project: https://github.com/fja05680/sp500/tree/master
    URL = "https://raw.githubusercontent.com/fja05680/sp500/refs/heads/master/S%26P%20500%20Historical%20Components%20%26%20Changes(08-17-2024).csv"
    df = pd.read_csv(URL)
    list_tickers = df["tickers"]
    for index in range(len(list_tickers)):
        list_tickers[index] = list_tickers[index].split(",")
    df["tickers"] = list_tickers
    df.to_pickle("github_datset.pkl")


df = pd.read_pickle("github_dataset.pkl")
print(len(df))
# print(df["tickers"][0])
# print(df.head(1))

# print(df["date"][len(df) - 2000: len(df) - 1])
print(df["date"][len(df) - 1800])
# print(len(df))
for ticker in df["tickers"][len(df) - 1800]:
    val = fmpsdk.company_profile(apikey=apikey, symbol=ticker)
    if val == []:
        print("Is not found: " + ticker)

