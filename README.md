# About
My project repo for collecting and analyzing market cap data from  1998-2025. The main output of this project will be a Tableau project conveying relevant insights alongside a folder with all the relevant data.

Link to final data after all the processing: [here](https://drive.google.com/file/d/12z38JBM2onLxSmxa7mkzrO41TeMIxP52/view?usp=sharing)

## Question to Answer from Project
1. What is the daily total market cap for SP500 from 1998-2025?
2. What is the number of companies in SP500 over time?
3. What is the top company by market cap in SP500 over time?
	3.1. What is the company's % composition of SP500 over time?
4. What is the % composition of tech/finance section in SP500 over time?
5. How accurate is our data?
	5.1. What data is missing overtime?
	5.2. Is our marketcap calculations accurate over time? 

## Notes/Overview of Project Data Gathering and Analysis
* Some stocks can repeatedly enter and exit SP500 under different circumstances
	* bankruptcy followed by company restructuring
	* renaming, either  ticker and/or company name
	* merger of two SP500 / separation of one company

* Code is split into different sections
	* Part 1 deals with data between 1998-01-01 to 2024-09-30
		* uses multiple sources for financial data listed in next section
	* Part 2 deals with data between 2024-10-01 to 2025-12-31
		* uses only data from FMP 
	* "Final" code focuses on combining the two above parts

* Part 1 code may be outdated
	* FMP API has moved from v3 to stable; v3 is discontinued.
	* fmpsdk python library is discontinued
	
* All final data used in stored into **"final data"** folder in parent directory


* Data collection is split into different parts/folder:
	* Daily Market cap is in **company_market_cap_data** folder
	* Company metadata is in **company_profiles** folder
	* Information about data sources for market cap data and amount of data collected is in **market_cap_metadata** folder
	* Files also exist for:
		* all trading days between 1998-2025
		* number of collected companies for each trading day
		* expected number of companies for each trading day
		* expected daily SP500 total market cap based on index value
			* makes assumption that SP500 market cap is $58 trillion at end of 2025 and that there is a linear relationship between index value and market cap 



* Data is initially stored in json files locally; They are converted into more efficient parquet files using AWS Glue.
	* file/notebook code used is in **SP500_Convert_to_Parquet_Finale.ipynb**
		* code is run in AWS Glue browser, not locally
		* will also need to store "final folder" in S3 bucket first
		* code is also available for doing part1 data gathering more efficiency in "part1_code" folder .ipynb files.
			* **NOTE**: image processing/analysis of stock charts may not work over cloud due to data lag/corruption
* After created the parquet files on AWS S3 with AWS Glue, they are queried using AWS Athena. The results of the queries and the queries themselves can be found in the "aws_athena_results" folder.


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

3. Run python files as needed 
		* No specific order of scripts can be given due to changes in APIs and libraries availability over time.

# Running Code on AWS
4. Set up AWS S3 by storing "final folder" into a bucket. Final version of folder can be found [here](https://drive.google.com/drive/folders/1jV2xlN1wLJ7dg3rLvGvHJC-JBX_lns0Z?usp=sharing)
5. Go to AWS Glue and upload **SP500_Convert_to_Parquet_Finale.ipynb**; change variable/file names as needs and run
6. Go to AWS Athena and run queries as stored in "aws_athena_results\SQL queries used.txt"


