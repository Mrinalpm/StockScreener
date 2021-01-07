from bselib.bse import BSE
import sqlite3
import requests
import re
import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import pandas as pd
import numpy as np
import pprint

# Global variables
# User defined variables

# link at which companies are available
stock_list_link = "https://s3.amazonaws.com/quandl-static-content/BSE%20Descriptions/stocks.txt"

KEYWORD = '|BOM'  # delimiter when reading company names
CRORE = "Cr"  # constant for crore
LAKH = "Lakh"  # constant for lakh
db = "companyData.db"  # name of database file
NULL = "NULL"
BSE_CODE_DIGITS = 6  # number of digits in a BSE stock code
DATE_FORMAT = "%d-%m-%Y"  # format of date used in bselib

BOMBAYSTOCKEXCHANGE = "BSE"
INDIA = "INDIA"
INR = "INR"

# The following are fields (dictionary keys) that exist in the json when data is read using
# the bselib. Dictionary values can be multi-level i.e. dictionaries within dictionaries and
# hence, related variables are grouped accordingly

DICT_KEY = "DICT_KEY"
MULTI_VALUE = "MULTI_VALUE"

quote_dict = {
    "CHANGE": {DICT_KEY: 'change', MULTI_VALUE: False},
    "DAYS_HIGH": {DICT_KEY: 'daysHigh', MULTI_VALUE: False},
    "DAYS_LOW": {DICT_KEY: 'daysLow', MULTI_VALUE: False},
    "FACE_VALUE": {DICT_KEY: 'faceValue', MULTI_VALUE: False},
    "FIFTY_TWO_WEEK_HIGH": {DICT_KEY: 'fiftytwo_WeekHigh', MULTI_VALUE: False},
    "FIFTY_TWO_WEEK_LOW": {DICT_KEY: 'fiftytwo_WeekLow', MULTI_VALUE: False},
    "FREE_FLOAT": {DICT_KEY: 'freeFloat', MULTI_VALUE: True},
    "GROUP": {DICT_KEY: 'group', MULTI_VALUE: False},
    "INDEX": {DICT_KEY: 'index', MULTI_VALUE: False},
    "LAST_OPEN": {DICT_KEY: 'lastOpen', MULTI_VALUE: False},
    "LTD": {DICT_KEY: 'ltd', MULTI_VALUE: False},
    "MARKET_CAP": {DICT_KEY: 'mktCap', MULTI_VALUE: False},
    "MONTH_HIGH_LOW": {DICT_KEY: 'monthHighLow', MULTI_VALUE: False},
    "PRICE_CHANGE": {DICT_KEY: 'pChange', MULTI_VALUE: False},
    "PREVIOUS_CLOSE": {DICT_KEY: 'previousClose', MULTI_VALUE: False},
    "SCRIPT_CODE": {DICT_KEY: 'scriptCode', MULTI_VALUE: False},
    "SECURITY_ID": {DICT_KEY: 'securityId', MULTI_VALUE: False},
    "STOCK_NAME": {DICT_KEY: 'stockName', MULTI_VALUE: False},
    "STOCK_PRICE": {DICT_KEY: 'stockPrice', MULTI_VALUE: False},
    "TOTAL_TRADED_QUANTITY": {DICT_KEY: 'totalTradedQty', MULTI_VALUE: True},
    "TOTAL_TRADED_VALUE": {DICT_KEY: 'totalTradedValue', MULTI_VALUE: True},
    "TWO_WEEK_AVERAGE_QUANTITY": {DICT_KEY: 'twoWeekAvgQty', MULTI_VALUE: True},
    "WEIGHTED_AVERAGE_PRICE": {DICT_KEY: 'wtdAvgPrice', MULTI_VALUE: True}
}

CHANGE = "change"
FACEVALUE = "faceValue"
FIFTYTWOWEEKHIGH = "fiftytwo_WeekHigh"
FIFTYTWOWEEKLOW = "fiftytwo_WeekLow"
STOCKPRICE = "stockPrice"
STOCKNAME = 'stockName'

DIVIDENDS = 'dividends'
DATA = 'data'
HEADER = 'header'
RECORDDATE = "Record Date"
DIVIDENDPERCENTAGE = 'Dividend Percentage'

MARKETCAP = 'mktCap'
VALUE = 'value'
IN = 'in'

PROFITRATIO = 'profit_ratio'
VALUERATIO = 'value_ratio'
PE = 'PE'
EPS = 'EPS'
INDUSTRY = 'Industry'
ROE = 'ROE'
TABLE = 'Table'
SCRIPCD = 'scrip_cd'

HEADERS = ["IDENTIFIER", "STOCKEXCHANGE", "CHANGE", "FACEVALUE", "FIFTYTWOWEEKHIGH", "FIFTYTWOWEEKLOW", "STOCKPRICE",
           "MARKETCAP", "STOCKNAME", "INDUSTRY", "EPS", "PE", "DIVIDENDYIELD", "FIVEYEARAVGDIVIDENDYIELD", "ROE"]
# Column names of the stockitem table in the db

# Dictionary keys when running a query
MIN = "Min"
MAX = "Max"

# ----------------------------------------------------------------------------------------------------------------------#
# Program variables
full_pattern = re.compile('1234567890.')  # number pattern

CRORE_VAL = 10000000  # value of one crore
LAKH_VAL = 100000  # value of one lakh

b = BSE()  # instance of BSE
initialized = False  # initialization of db tables, initially false
conn = sqlite3.connect(db)


temp_dict = dict()

"""
Function for getting 6 digit codes from the BSE
Returns list containing codes
"""


def get_bse_codes():
    global BSE_CODE_DIGITS, stock_list_link, KEYWORD
    to_return = []  # list to be returned
    data = requests.get(stock_list_link).text.splitlines()  # get data as array of lines
    for i in data:
        if KEYWORD in i:
            to_add = i.split(KEYWORD)[1]  # the 6-digit code
            if len(to_add) == BSE_CODE_DIGITS:
                to_return.append(to_add)
    return to_return

"""
Function for validating a dpuble in the form of a string
If valid, returns the double value as a string, else returns NULL
"""


def validate_double(str_param):
    global NULL

    # noinspection PyBroadException
    try:
        to_return = str(float(str_param.replace(',', '')))  # if any commas exist, remove them
    except Exception:  # exception occured, just return NULL
        to_return = NULL
    return to_return


"""
Gets the date formatted as dd-mm-YYYY
"""


def get_date(date):
    global DATE_FORMAT
    return datetime.strptime(date, DATE_FORMAT)


"""
Returns a look up table in the form of a dictionary
Input: A list containing the headers
Return: A dictionary containing corresponding indexes for each of the header values
"""


def get_lookup_table(param_list):
    to_return = dict()

    for i in range(len(param_list)):
        to_return[param_list[i]] = i

    return to_return


"""
Gets the current dividend yield and the 5 year average yield of a given stock
This is specifically for the stocks on the BSE
Input parameters: 6 digit BSE stock code (identifier)
                  Price: Current price of the stock
                  Face Value: Current face value of the stock
"""


def handle_dividend(identifier, price, face_value):
    global DIVIDENDS, DATA, RECORDDATE, DIVIDENDPERCENTAGE, HEADER, NULL
    data = b.corporate_actions(identifier)
    end_of_fin_yr = datetime.now()

    # each of the previous 5 year slabs. This will contain dividend percentages
    prev_yr_totals = [0.0, 0.0, 0.0, 0.0, 0.0]

    fiveyryield = NULL
    currentyield = NULL

    if DIVIDENDS in data:
        if DATA in data[DIVIDENDS]:
            dictionary = get_lookup_table(data[DIVIDENDS][HEADER])  # get the lookup table
            for i in data[DIVIDENDS][DATA]:
                date = get_date(i[dictionary[RECORDDATE]])  # date of dividend issued

                if end_of_fin_yr - relativedelta(years=1) < date <= end_of_fin_yr:
                    prev_yr_totals[0] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    # dividend issued current year
                elif end_of_fin_yr - relativedelta(years=2) < date <= end_of_fin_yr - relativedelta(years=1):
                    prev_yr_totals[1] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    # dividend issued previous year
                elif end_of_fin_yr - relativedelta(years=3) < date <= end_of_fin_yr - relativedelta(years=2):
                    prev_yr_totals[2] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    # dividend issued two years ago
                elif end_of_fin_yr - relativedelta(years=4) < date <= end_of_fin_yr - relativedelta(years=3):
                    prev_yr_totals[3] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    # dividend issued three years ago
                elif end_of_fin_yr - relativedelta(years=5) < date <= end_of_fin_yr - relativedelta(years=4):
                    prev_yr_totals[4] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    # dividend issued four years ago

            # five year yield returned as a percentage
            fiveyryield = sum(prev_yr_totals) * float(face_value) / (5.0 * float(price))
            currentyield = prev_yr_totals[0] * float(face_value) / float(
                price)  # current yield returned as a percentage

    return currentyield, fiveyryield

def main():
    companies = get_bse_codes()  # get list of companies

    QUOTE = "quote"

    HISTORICAL_FINANCIAL_STATEMENT = "historical_financial_statement"
    FINANCIAL_STATEMENT = "financial_statement"
    STATEMENT_ANALYSIS = "statement_analysis"
    BALANCE_SHEET = "balancesheet"
    YOY_RESULTS = "yoy_results"
    QUARTER_RESULTS = "quarter_results"
    CASHFLOW = "cashflow"

    PERFORMANCE_ANALYSIS = "performance_analysis"

    FINANCIAL_RATIOS = "financial_ratios"

    PEER_COMPARISON = "peer_comparison"

    CORPORATE_ACTIONS = "corporate_actions"

    HOLDING_INFO_ANALYSIS = "shareholding_info_and_analysis"

    progressbar = tqdm.tqdm(companies)
    count = 0
    for i in progressbar:
        main_dict = dict()
        if i not in temp_dict:

            main_dict[QUOTE] = b.quote(i)

            """main_dict[FINANCIAL_STATEMENT] = {}
            main_dict[FINANCIAL_STATEMENT][BALANCE_SHEET] = b.statement(i, stats=BALANCE_SHEET)
            main_dict[FINANCIAL_STATEMENT][YOY_RESULTS] = b.statement(i, stats=YOY_RESULTS)
            main_dict[FINANCIAL_STATEMENT][QUARTER_RESULTS] = b.statement(i, stats=QUARTER_RESULTS)
            main_dict[FINANCIAL_STATEMENT][CASHFLOW] = b.statement(i, stats=CASHFLOW)

            main_dict[HISTORICAL_FINANCIAL_STATEMENT] = {}

            main_dict[HISTORICAL_FINANCIAL_STATEMENT][BALANCE_SHEET] = b.historical_stats(i, stats=BALANCE_SHEET)
            main_dict[HISTORICAL_FINANCIAL_STATEMENT][YOY_RESULTS] = b.historical_stats(i, stats=YOY_RESULTS)
            main_dict[HISTORICAL_FINANCIAL_STATEMENT][QUARTER_RESULTS] = b.historical_stats(i, stats=QUARTER_RESULTS)
            main_dict[HISTORICAL_FINANCIAL_STATEMENT][CASHFLOW] = b.historical_stats(i, stats=CASHFLOW)

            main_dict[STATEMENT_ANALYSIS] = {}
            main_dict[STATEMENT_ANALYSIS][BALANCE_SHEET] = b.stmt_analysis(i, stats=BALANCE_SHEET)
            main_dict[STATEMENT_ANALYSIS][YOY_RESULTS] = b.stmt_analysis(i, stats=YOY_RESULTS)
            main_dict[STATEMENT_ANALYSIS][QUARTER_RESULTS] = b.stmt_analysis(i, stats=QUARTER_RESULTS)
            main_dict[STATEMENT_ANALYSIS][CASHFLOW] = b.stmt_analysis(i, stats=CASHFLOW)"""

            main_dict[PERFORMANCE_ANALYSIS] = b.analysis(i)

            main_dict[FINANCIAL_RATIOS] = b.ratios(i)

            main_dict[PEER_COMPARISON] = b.peers(i)

            main_dict[CORPORATE_ACTIONS] = b.corporate_actions(i)

            main_dict[HOLDING_INFO_ANALYSIS] = b.holdings(i)

        temp_dict[i] = main_dict

        count +=1

        if  count % 10 == 0:
            write_to_file()
            count = 0
        progressbar.set_description("Reading companies")

def write_to_file():
    # Open a file: file
    file = open('result.json', mode='r')

    # read all lines at once
    all_of_it = file.read()

    # close the file
    file.close()

    open('result.json', 'w').close()
    try:
        with open('result.json', 'w') as fp:
            json.dump(temp_dict, fp, allow_nan=True)
        fp.close()
    except Exception:
        with open('result.json', 'w') as fp:
            fp.write(all_of_it)
        fp.close()

try:
     main()
     write_to_file()
except Exception as e:
    print(str(e))
    write_to_file()
