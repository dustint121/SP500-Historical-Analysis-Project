# About
My project repo for collecting and analyzing market cap data from  1998-2025. The main output of this project will be a Tableau project conveying relevant insights alongside a folder with all the relevant data.

Notes:
* Some stocks can repeatedly enter and exit SP500 under different circumstances
	* bankruptcy followed by company restructuring
	* renaming
	* merger of two SP500 / separation of one company

## List of Finance/Stock Websites Used for Project

* [Financial Modeling Prep](https://site.financialmodelingprep.com/ )
	* API (need standard subscription at $30 monthly)
		* get current SP500 companies and marketcap
		* get history of previous SP500 companies
		* get data for each SP500 company
		
* [Tiingo](https://www.tiingo.com/)
	* API (need Tiingo Fundamentals 20+ Years subscription at $50 monthly)
		* get SP500 company and marketcap
		* get data for each SP500 company
		
* [Kibot](https://www.kibot.com/)
	* API  (need EOD subscription at $16 monthly)
		* get SP500 company marketcap
* [Finchat.io --> Fiscal.ai](https://fiscal.ai/)
	* Screenshotted stock charts to calculate marketcap
* https://companiesmarketcap.com/
	* webscrapped website for some companies and marketcap data
* https://www.investing.com/ 
	* download csv files for relevant stocks
* https://eoddata.com/
	* download csv files for relevant stocks
	* note: may be inaccurate
* https://www.marketwatch.com/investing/index/spx/download-data
	* get daily data for SP500(SPX) index value
* https://i.dell.com/sites/csdocuments/Corporate_secure_Documents/en/dell-closing-costs.pdf
	* for Dell stock before structuring  




 ## Other Acknowledgements 
 Used csv from https://github.com/fja05680/sp500/tree/master to have a second form of validity for SP500 company tickers since 1996.

# Instructions for Running Code Repo on Local Machine

## In Project File after git cloning

1. Add **.env** file with

> fmp_apikey=**[api key given by FinancialModelingPrep]**

> tiingo_token =**[token given by Tiingo when setting up API]**

> kibot_user=**[kibot email]**

> kibot_password=**[kibot password]**

2. Run
>pip install -r requirements.txt


