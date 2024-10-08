import os
from dotenv import load_dotenv
import fmpsdk
import pandas as pd

load_dotenv()
apikey = os.environ.get("apikey")

sp_500_stocks_1996 = pd.read_pickle("github_dataset.pkl")["date"]


print(len(sp_500_stocks_1996))


#need sectors for these stocks
# ['FLIR', 'VAR', 'CXO', 'TIF', 'NBL', 'ETFC', 'AGN', 'RTN', 'WCG', 'STI', 'VIAB', 'CELG', 'TSS', 'APC', 'RHT']



print(sp_500_stocks_1996[1142])
print(sp_500_stocks_1996[1143])

tickers = pd.read_pickle("github_dataset.pkl")["tickers"]

tickers_1 = tickers[1142]
tickers_2 = tickers[1143]

for ticker in tickers_1:
    if ticker not in tickers_2:
        print(ticker)