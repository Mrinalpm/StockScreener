#Variable values for the BSE Stock Screener

# link at which companies are available
stock_list_link = "https://s3.amazonaws.com/quandl-static-content/BSE%20Descriptions/stocks.txt"

#Backup file containing stocks if the above link doesn't work
stock_list_file = "stocks.txt"

# name of file in which stock data will be stored after being read
file_name = "result.json"

#name of spreadsheet file
excel_file_name = 'stock_list.xlsx'

# delimiter when reading company names (this delimiter is according to what is at stock_list_link)
KEYWORD = '|BOM'

# number of digits in a BSE stock code
BSE_CODE_DIGITS = 6

#Null keyword. Used if data wasn't retrieved for a field
NULL = "NULL"

#The format of date
date_format = "%d-%m-%Y"

#Dictionary keys for storing data (user-defined)
QUOTE = "quote"
PERFORMANCE_ANALYSIS = "performance_analysis"
FINANCIAL_RATIOS = "financial_ratios"
PEER_COMPARISON = "peer_comparison"
CORPORATE_ACTIONS = "corporate_actions"
HOLDING_INFO_ANALYSIS = "shareholding_info_and_analysis"

#----------------------------------------------------------------------------------------------------------------------#
#The following are dictionary keys present. This is what's present in the data from bselib

QUOTE_STOCK_NAME = "stockName"
QUOTE_STOCK_PRICE = "stockPrice"
QUOTE_FACE_VALUE = "faceValue"
QUOTE_PERCENT_CHANGE = "pChange"
QUOTE_FIFTY_HIGH = "fiftytwo_WeekHigh"
QUOTE_FIFTY_LOW = "fiftytwo_WeekLow"
QUOTE_MONTH_HIGH_LOW = "monthHighLow"

QUOTE_MARKET_CAP = "mktCap"
QUOTE_MARKET_CAP_IN = "in"
QUOTE_MARKET_CAP_VALUE = "value"

FINANCIAL_RATIOS_PROFIT_RATIO = "profit_ratio"
FINANCIAL_RATIOS_VALUE_RATIO = "value_ratio"

FINANCIAL_RATIOS_FACE_VALUE = "FaceVal"
FINANCIAL_RATIOS_EPS = "EPS"
FINANCIAL_RATIOS_CEPS = "CEPS"
FINANCIAL_RATIOS_PE = "PE"
FINANCIAL_RATIOS_ROE = "ROE"

PEER_COMPARISON_TABLE = "Table"
PEER_COMPARISON_SCRIP_CD = "scrip_cd"
PEER_COMPARISON_EPS = "EPS"
PEER_COMPARISON_PE = "PE"
PEER_COMPARISON_CEPS = "Cash_EPS"
PEER_COMPARISON_FACE_VALUE = "FACE_VALUE"
PEER_COMPARISON_NAME = "Name"
PEER_COMPARISON_LTP = "LTP"

CORPORATE_ACTIONS_DIVIDENDS = "dividends"
CORPORATE_ACTIONS_DIVIDENDS_DATA = "data"
CORPORATE_ACTIONS_DIVIDENDS_HEADER = "header"
CORPORATE_ACTIONS_DIVIDENDS_RECORD_DATE = "Record Date"
CORPORATE_ACTIONS_DIVIDENDS_DIVIDEND_PERCENTAGE = "Dividend Percentage"

HOLDING_INFO_ANALYSIS_HOLDINGS = "holdings"
HOLDING_INFO_ANALYSIS_DATA = "data"
HOLDING_INFO_ANALYSIS_PIE = "pie"
HOLDING_INFO_ANALYSIS_MUTUAL_FUNDS = "mutual_funds"
HOLDING_INFO_ANALYSIS_INSURANCE = "insurance"
HOLDING_INFO_ANALYSIS_FII = "fiis"
HOLDING_INFO_ANALYSIS_PROMOTERS = "promoters"
HOLDING_INFO_ANALYSIS_OTHER = "other"
HOLDING_INFO_ANALYSIS_NON_INSTITUTION = "non_institution"
HOLDING_INFO_ANALYSIS_OTHER_DIIS = "other_diis"
HOLDING_INFO_ANALYSIS_PERC = 'perc'

#Column headers in the excel file
headers = ["Stock Code", "Name", "Price", "% change", "52 Week High", "52 Low High",
           "Month High/Low", "Face Value", "Market Cap", "P/E", "EPS",
           "CEPS", "ROE", "Dividend Yield", "5 year avg. div. yield",
           "Mutual Fund shareholding %", "Insurance companies shareholding %", "Foreign Investors shareholding %",
           "Promoters shareholding %", "Other shareholding %", "Non-institution shareholding %"
           , "Other DIIs shareholding %"]

# Number constants
number_suffix = {"Cr": 10000000, "Lac": 100000}