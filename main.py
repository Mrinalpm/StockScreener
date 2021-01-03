from bselib.bse import BSE
import sqlite3
import requests
import re
import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

#Global variables
#User defined variables

#link at which companies are available
stock_list_link = "https://s3.amazonaws.com/quandl-static-content/BSE%20Descriptions/stocks.txt"

KEYWORD = '|BOM'  #delimiter when reading company names
CRORE = "Cr"  #constant for crore
LAKH = "Lakh"  #constant for lakh
db = "companyData.db"  #name of database file
NULL = "NULL"
BSE_CODE_DIGITS = 6  #number of digits in a BSE stock code
DATE_FORMAT = "%d-%m-%Y"  #format of date used in bselib

BOMBAYSTOCKEXCHANGE = "BSE"
INDIA = "INDIA"
INR = "INR"

#The following are fields (dictionary keys) that exist in the json when data is read using
#the bselib. Dictionary values can be multi-level i.e. dictionaries within dictionaries and
#hence, related variables are grouped accordingly

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
#Column names of the stockitem table in the db

#Dictionary keys when running a query
MIN = "Min"
MAX = "Max"

#----------------------------------------------------------------------------------------------------------------------#
#Program variables
full_pattern = re.compile('1234567890.')  #number pattern

CRORE_VAL = 10000000  #value of one crore
LAKH_VAL = 100000  #value of one lakh

b = BSE()  #instance of BSE
initialized = False  #initialization of db tables, initially false
conn = sqlite3.connect(db)

"""
Function for getting 6 digit codes from the BSE
Returns list containing codes
"""


def get_bse_codes():
    global BSE_CODE_DIGITS, stock_list_link, KEYWORD
    to_return = []  #list to be returned
    data = requests.get(stock_list_link).text.splitlines()  #get data as array of lines
    for i in data:
        if KEYWORD in i:
            to_add = i.split(KEYWORD)[1]  #the 6-digit code
            if len(to_add) == BSE_CODE_DIGITS:
                to_return.append(to_add)
    return to_return


"""
Function for creating database tables
Called when initializing the database
global initialized will be set to true if table creation was successful
"""


def create_tables():
    global initialized, conn

    conn.execute('''CREATE TABLE EXCHANGE
        (EXCHANGENAME TEXT PRIMARY KEY   NOT NULL,
        COUNTRY     TEXT,
        CURRENCY    TEXT);''')

    conn.execute('''CREATE TABLE STOCKITEM
        (IDENTIFIER TEXT,
        STOCKEXCHANGE TEXT,
        CHANGE DECIMAL,
        FACEVALUE DECIMAL,
        FIFTYTWOWEEKHIGH DECIMAL,
        FIFTYTWOWEEKLOW DECIMAL,
        STOCKPRICE DECIMAL,
        MARKETCAP DECIMAL,
        STOCKNAME TEXT,
        INDUSTRY TEXT,
        EPS DECIMAL,
        PE DECIMAL,
        DIVIDENDYIELD DECIMAL,
        FIVEYEARAVGDIVIDENDYIELD DECIMAL,
        ROE DECIMAL,
        PRIMARY KEY(IDENTIFIER, STOCKEXCHANGE));''')

    initialized = True


"""
Function for validating a dpuble in the form of a string
If valid, returns the double value as a string, else returns NULL
"""


def validate_double(str_param):
    global NULL

    # noinspection PyBroadException
    try:
        to_return = str(float(str_param.replace(',', '')))  #if any commas exist, remove them
    except Exception:  #exception occured, just return NULL
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
            dictionary = get_lookup_table(data[DIVIDENDS][HEADER])  #get the lookup table
            for i in data[DIVIDENDS][DATA]:
                date = get_date(i[dictionary[RECORDDATE]])  #date of dividend issued

                if end_of_fin_yr - relativedelta(years=1) < date <= end_of_fin_yr:
                    prev_yr_totals[0] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued current year
                elif end_of_fin_yr - relativedelta(years=2) < date <= end_of_fin_yr - relativedelta(years=1):
                    prev_yr_totals[1] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued previous year
                elif end_of_fin_yr - relativedelta(years=3) < date <= end_of_fin_yr - relativedelta(years=2):
                    prev_yr_totals[2] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued two years ago
                elif end_of_fin_yr - relativedelta(years=4) < date <= end_of_fin_yr - relativedelta(years=3):
                    prev_yr_totals[3] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued three years ago
                elif end_of_fin_yr - relativedelta(years=5) < date <= end_of_fin_yr - relativedelta(years=4):
                    prev_yr_totals[4] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued four years ago

            # five year yield returned as a percentage
            fiveyryield = sum(prev_yr_totals) * float(face_value) / (5.0 * float(price))
            currentyield = prev_yr_totals[0] * float(face_value) / float(price)  #current yield returned as a percentage

    return currentyield, fiveyryield


"""
Function for adding the details of a particular stock to the database
Specifically for BSE stocks only
Input: 6-digit BSE stock code
"""


def insert_bse_data(identifier):
    global initialized, STOCKNAME, EPS, PE, INDUSTRY, ROE, VALUERATIO, PROFITRATIO, CRORE, CRORE_VAL, LAKH, LAKH_VAL
    global BOMBAYSTOCKEXCHANGE, b, NULL, CHANGE, FACEVALUE, FIFTYTWOWEEKHIGH, FIFTYTWOWEEKLOW, STOCKPRICE, MARKETCAP
    global VALUE, IN, TABLE, SCRIPCD, conn

    c = conn.cursor()
    stockexchange = BOMBAYSTOCKEXCHANGE

    c.execute('SELECT * FROM STOCKITEM WHERE IDENTIFIER = ? AND STOCKEXCHANGE = ?', (identifier, stockexchange,))

    if initialized and len(c.fetchall()) == 0:  #if tables have been created and stock item doesn't exist already in db
        data = b.quote(identifier)

        change = NULL
        if CHANGE in data:
            change = validate_double(data[CHANGE])

        face_value = NULL
        if FACEVALUE in data:
            face_value = validate_double(data[FACEVALUE])

        fifty_two_week_high = NULL
        if FIFTYTWOWEEKHIGH in data:
            fifty_two_week_high = str(data[FIFTYTWOWEEKHIGH])

        fifty_two_week_low = NULL
        if FIFTYTWOWEEKLOW in data:
            fifty_two_week_low = str(data[FIFTYTWOWEEKLOW])

        stock_price = NULL
        if STOCKPRICE in data:
            stock_price = validate_double(data[STOCKPRICE])

        market_cap = NULL
        if MARKETCAP in data:
            market_cap = validate_double(data[MARKETCAP][VALUE])
            if NULL not in market_cap:
                if CRORE in data[MARKETCAP][IN]:
                    market_cap = float(market_cap) * CRORE_VAL
                if LAKH in data[MARKETCAP][IN]:
                    market_cap = float(market_cap) * LAKH_VAL
        market_cap = str(market_cap)

        stock_name = NULL
        if STOCKNAME in data:
            stock_name = data[STOCKNAME].replace('\'', '"')

        fiveyryield = NULL
        currentyield = NULL

        if NULL not in stock_price and NULL not in face_value:
            my_list = handle_dividend(identifier, stock_price, face_value)
            currentyield = str(my_list[0])
            fiveyryield = str(my_list[1])

        eps = NULL
        pe = NULL
        industry = NULL
        roe = NULL
        data = b.ratios(identifier)

        if PROFITRATIO in data:  #attempt to get from ratios
            if PE in data[PROFITRATIO]:
                pe = str(data[PROFITRATIO][PE])
            if EPS in data[PROFITRATIO]:
                eps = str(data[PROFITRATIO][EPS])
        if VALUERATIO in data:
            if INDUSTRY in data[VALUERATIO]:
                industry = str(data[VALUERATIO][INDUSTRY]).replace('\'', '"')
            if ROE in data[VALUERATIO]:
                roe = str(data[VALUERATIO][ROE])

        data = b.peers(identifier)

        if NULL in eps and TABLE in data and EPS in data[TABLE][0]:  #eps still null
            if SCRIPCD in data[TABLE][0] and data[TABLE][0][SCRIPCD] == float(identifier):
                eps = str(data[TABLE][0][EPS])

        if NULL in pe and TABLE in data and PE in data[TABLE][0]:
            if SCRIPCD in data[TABLE][0] and data[TABLE][0][SCRIPCD] == float(identifier):
                pe = str(data[TABLE][0][PE])

        try:
            c.execute('SELECT * FROM EXCHANGE WHERE EXCHANGENAME = ?', (stockexchange,))
            if len(c.fetchall()) == 0:
                #insert
                c.execute('INSERT INTO EXCHANGE VALUES (?, ?, ?)', (BOMBAYSTOCKEXCHANGE, INDIA, INR))
        except Exception as ex:
            print(str(ex))

        c.execute('SELECT * FROM EXCHANGE WHERE EXCHANGENAME = ?', (stockexchange,))
        if len(c.fetchall()) != 0:
            #ensure table exists

            conn.execute("INSERT INTO STOCKITEM VALUES ('" + identifier + "', '" + stockexchange + "', " + change +
                         ', ' + face_value + ', ' + fifty_two_week_high + ', ' + fifty_two_week_low + ', ' +
                         stock_price + ', ' + market_cap + ", '" + stock_name + "', '" + industry + "', " +
                         eps + ', ' + pe + ', ' + currentyield + ', ' + fiveyryield + ', ' + roe + ');')


def print_stats():
    global conn, HEADERS

    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'STOCKITEM';")

    length = len(c.fetchall())
    if length == 1:  #table exists
        to_print = "Number of missing fields:\n"
        c.execute("SELECT COUNT(*) FROM STOCKITEM;")
        result = c.fetchall()
        assert (len(result) == 1)
        size = result[0][0]
        for field in HEADERS:
            query = "SELECT COUNT(*) FROM STOCKITEM WHERE " + field + " IS ? OR " + field + " IS ?;"
            c.execute(query, (None, "NULL",))
            result = c.fetchall()
            assert (len(result) == 1)
            count = result[0][0]
            to_print += (field + ": " + str(count) + "/" + str(size) + "\n")
        print(to_print)


def get_decimal_list(index):
    global HEADERS

    to_return = []
    assert (index < len(HEADERS))
    query = "SELECT * FROM STOCKITEM "

    if len(index) > 0:
        query += "WHERE "

    dict_count = 0

    for entry in index:
        dict_count += 1
        if entry in HEADERS:
            query += ("NOT " + str(entry) + " IS ? AND NOT" + str(entry) + " IS NULL AND " + str(entry) + " BETWEEN " +
                      str(index[entry][MIN]) + " AND " + str(index[entry][MAX]))
            if dict_count == len(index):
                query += " AND "
    return to_return


def main():
    global initialized, conn
    companies = get_bse_codes()  #get list of companies
    #createTables(conn)  #create db tables
    initialized = True
    if initialized:  #proceed only if initialized
        progressbar = tqdm.tqdm(companies)
        for i in progressbar:
            insert_bse_data(i)
            progressbar.set_description("Reading companies")
            conn.commit()


try:
    #main(conn, c)
    pd.set_option('display.max_columns', None)
    print_stats()
    k = 11
    # c = conn.cursor()

    #print(pd.read_sql_query("SELECT * FROM STOCKITEM WHERE NOT ? IS ? LIMIT 20", conn,None, True,
    # ("FIVEYEARAVGDIVIDENDYIELD", None, )))

    #print(len(c.fetchall()))
except Exception as e:
    print(str(e))