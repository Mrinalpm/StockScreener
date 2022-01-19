# BSE Stock Analyzer
## Background

This is a stock analyzer for the Bombay Stock Exchange (BSE).
This analyzer stands out due to the following features:
* Dividend yield and past year averages

	For long term investors, stocks with high dividend yield is very important.  The feature to sort companies by their dividend yield was lacking in a lot of stock screeners I came across. In addition to having this, this analyzer also provides the average yield over the past 5 years to ensure consistency of dividend payout by the company.

* Shareholding Pattern

	A good company to invest in can also be determined based on its shareholding pattern. I concluded that a higher shareholding % by mutual funds and insurance companies is generally indicative of a good company to invest in. This analyzer provides the complete shareholding pattern including mutual fund, insurance companies, foreign investors and non-institutional shareholding patterns.
	
## More about this tool
The script uses **bselib**, an API for fetching BSE stock data. Documentation can be found [here](https://bselib.readthedocs.io/en/latest/). 

The analyzer fetches the following data:
* 6 digit BSE stock code
* Stock Name
* Stock Price
* % change
* 52 week high
* 52 week low
* Month high/low
* Face Value
* Market Capitalization
* P/E
* EPS
* CEPS
* ROE
* Dividend Yield
* 5 year average dividend yield
* Shareholding pattern (Mutual funds, insurance, 
foreign investors, promoters, non institution, others)

The fetched data is stored in a json file after which, an excel spreadsheet can be generated. These file names can be specified as indicated below:

![img1](https://user-images.githubusercontent.com/55770671/104220254-d8de9400-5404-11eb-8043-36845d4fa96f.PNG)

**Update**: As of v2.0, all fields extracted through bselib are stored in the excel spreadsheet, not just the ones from the above list

Due to the amount of data involved, the analyzer can't run in real-time. Instead, it first fetches data and stores data in the json file. After this, the analyzer is run and the spreadsheet can be generated based on the data from the json.

Once the spreadsheet is generated, the data can be analyzed for shortlisting companies. Here is a sample screenshot of the spreadsheet:

![img2](https://user-images.githubusercontent.com/55770671/104220339-f875bc80-5404-11eb-893a-e612910cc5e5.PNG)

![img3](https://user-images.githubusercontent.com/55770671/104220344-f9a6e980-5404-11eb-81ab-3d0fccda6ee2.PNG)

If data couldn't be retrieved for a certain field, a **NULL** value is entered in that cell.

**Note**: You may notice the the dividend yield of a particular company and compare it with the result of a google search to find that it is different. The yield google shows assume the financial year starts on 1st January whereas the analyzer assumes the financial year starts on 1st April (the actual date marking the start of the Indian financial year), hence the discrepancy.

## Technical Notes

These are technical notes used to explain the codebase starting v2.0 release

Code has been divided up into three blocks, each designed to fetch data based on input from the previous block.

**Block 1:**

**Input:** A CSV containing all active equities on the BSE

Input into this block will be done manually. Download this CSV from https://www.bseindia.com/corporates/List_Scrips.html (Segment: Equity; Status: Active). Name the CSV as defined under variables.py

**Output:** A list of 6-digit BSE codes

**Block 2:**

**Input:** A list of 6-digit BSE codes

**Output:** A .json file containing ALL the data of the companies

**Block 3:**

**Input:** A .json file containing ALL the data of the companies

**Output:** A spreadsheet file containing ALL the data of the companies
## Contact
For more information, please [contact me](https://www.linkedin.com/in/mrinal-managoli-442bb0170/) on LinkedIn: