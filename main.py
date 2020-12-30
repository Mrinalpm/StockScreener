from bselib.bse import BSE
import sqlite3
import requests
import re
import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta

#Global variables
#User defined variables

stock_list_link = "https://s3.amazonaws.com/quandl-static-content/BSE%20Descriptions/stocks.txt" #link at which companies are available
KEYWORD = '|BOM' #delimiter when reading company names
CRORE = "Cr" #constant for crore
LAKH = "Lakh" #constant for lakh
db = "companyData.db" #name of database file
NULL = "NULL"
BSE_CODE_DIGITS = 6 #number of digits in a BSE stock code
DATE_FORMAT = "%d-%m-%Y" #format of date used in bselib

BOMBAYSTOCKEXCHANGE = "BSE"
INDIA = "INDIA"
INR = "INR"

#The following are fields (dictionary keys) that exist in the json when data is read using the bselib
#Dictionary values can be multi-level i.e. dictionaries within dictionaries and hence, related variables are grouped accordingly
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

HEADERS = ["IDENTIFIER", "STOCKEXCHANGE", CHANGE, FACEVALUE, FIFTYTWOWEEKHIGH, FIFTYTWOWEEKLOW, STOCKPRICE, MARKETCAP, STOCKNAME, INDUSTRY, EPS, PE, "DIVIDENDYIELD", "FIVEYEARAVGDIVIDENDYIELD", ROE]
#identifier, stockexchange, change, fv, ftwh, ftwl, stockprice, marketcap, stockname, industry, eps, pe, divyield, fiveyearyield, roe
stockAttributes = [0] * len(HEADERS)

#----------------------------------------------------------------------------------------------------------------------------------------------------#
#Program variables
full_pattern = re.compile('1234567890.') #number pattern

CRORE_VAL = 10000000 #value of one crore
LAKH_VAL = 100000 #value of one lakh

b = BSE() #instance of BSE
initialized = False #initialization of db tables, initially false

#Variables tracking how much info couldn't be read
totalCompanies = 0 #total number of companies available
companiesWithMissingInfo = 0 #Number of companies with missing info (A single company might have >1 field missing, still counts as 1)
missingFields = 0 #Total number of missing fields from all companies

conn = sqlite3.connect(db)
c = conn.cursor()

"""
Function for getting 6 digit codes from the BSE
Returns list containing codes
"""
def getBSECodes():
    global totalCompanies, BSE_CODE_DIGITS
    toReturn = [] #list to be returned
    data = requests.get(stock_list_link).text.splitlines() #get data as array of lines
    for i in data:
        if KEYWORD in i:
            toAdd = i.split(KEYWORD)[1] #the 6-digit code
            if len(toAdd) == BSE_CODE_DIGITS:
                toReturn.append(toAdd)
                totalCompanies += 1
    return toReturn

"""
Function for creating database tables
Called when initializing the database
global initialized will be set to true if table creation was successful
"""
def createTables():
    global initialized
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
        PRIMARY KEY(IDENTIFIER, STOCKEXCHANGE));'''  )

    initialized = True

"""
Function for validating a dpuble in the form of a string
If valid, returns the double value as a string, else returns NULL
"""
def validateDouble(strParam):
    global NULL
    toReturn = NULL

    try:
        toReturn = str(float(strParam.replace(',',''))) #if any commas exist, remove them
    except Exception as e: #exception occured, just return NULL
        toReturn = NULL
    return toReturn

"""
Gets the date formatted as dd-mm-YYYY
"""
def getDate(date):
    global DATE_FORMAT
    return datetime.strptime(date, DATE_FORMAT)
"""
Returns a look up table in the form of a dictionary
Input: A list containing the headers
Return: A dictionary containing corresponding indexes for each of the header values
"""
def getLUTTable(list):
    toReturn = dict()

    for i in range(len(list)):
        toReturn[list[i]] = i

    return toReturn

"""
Gets the current dividend yield and the 5 year average yield of a given stock
This is specifically for the stocks on the BSE
Input parameters: 6 digit BSE stock code (identifier)
                  Price: Current price of the stock
                  Face Value: Current face value of the stock
"""
def handleDividend(identifier, price, faceValue):
    global DIVIDENDS, DATA, RECORDDATE, DIVIDENDPERCENTAGE, HEADER
    data = b.corporate_actions(identifier)
    endOfFinYr = datetime.now()
    prevYearTotals = [0.0, 0.0, 0.0, 0.0, 0.0] #each of the previous 5 year slabs. This will contain dividend percentages
    fiveyryield = NULL
    currentyield = NULL

    if DIVIDENDS in data:
        if DATA in data[DIVIDENDS]:
            dictionary = getLUTTable(data[DIVIDENDS][HEADER]) #get the lookup table
            for i in data[DIVIDENDS][DATA]:
                date = getDate(i[dictionary[RECORDDATE]]) #date of dividend issued

                if date <= endOfFinYr and date > endOfFinYr - relativedelta(years=1):
                    prevYearTotals[0] += float (i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued current year
                elif date <= endOfFinYr - relativedelta(years=1) and date > endOfFinYr - relativedelta(years=2):
                    prevYearTotals[1] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued previous year
                elif date <= endOfFinYr - relativedelta(years=2) and date > endOfFinYr - relativedelta(years=3):
                    prevYearTotals[2] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued two years ago
                elif date <= endOfFinYr - relativedelta(years=3) and date > endOfFinYr - relativedelta(years=4):
                    prevYearTotals[3] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued three years ago
                elif date <= endOfFinYr - relativedelta(years=4) and date > endOfFinYr - relativedelta(years=5):
                    prevYearTotals[4] += float(i[dictionary[DIVIDENDPERCENTAGE]].replace('%', ''))
                    #dividend issued four years ago

            fiveyryield = sum(prevYearTotals) * float(faceValue) / (5.0 * float(price))  # five year yield returned as a percentage
            currentyield = prevYearTotals[0] * float(faceValue) / float(price)  # current yield returned as a percentage

    return currentyield, fiveyryield

"""
Function for returning an SQL query
Input: Array containing the relevant fields AS STRINGS
The size of input array and the order of the fields must match the global array defined for field headers
"""
def getQuery(fields):
    global companiesWithMissingInfo, HEADERS
    assert (len(HEADERS) == len(fields))
    missingInfo = False #boolean checking if company has any missing fields
    for i in range(len(fields)):
        if NULL in fields[i]: #missing info
            stockAttributes[i] += 1
            missingInfo = True

    if missingInfo:
        companiesWithMissingInfo += 1

    return "INSERT INTO STOCKITEM VALUES ('"+ fields[0] + "', '"+ fields[1] + "', "+ fields[2]+ ', '+ fields[3] + ', '+ fields[4] + ', '+ fields[5] + ', '+ fields[6] + ', '+ fields[7] + ", '"+ fields[8] + "', '"+ fields[9] + "', "+ fields[10]+ ', '+ fields[11] + ', '+ fields[12] + ', '+ fields[13] + ', '+ fields[14] + ');'

"""
Function for adding the details of a particular stock to the database
Specifically for BSE stocks only
Input: 6-digit BSE stock code
"""
def insertBSEData(identifier):
    global initialized, STOCKNAME, EPS, PE, INDUSTRY, ROE, VALUERATIO, PROFITRATIO, CRORE, CRORE_VAL, LAKH, LAKH_VAL, BOMBAYSTOCKEXCHANGE, c, b
    global NULL, CHANGE, FACEVALUE, FIFTYTWOWEEKHIGH, FIFTYTWOWEEKLOW, STOCKPRICE, MARKETCAP, VALUE, IN, TABLE, SCRIPCD

    stockexchange = BOMBAYSTOCKEXCHANGE
    c.execute('SELECT * FROM STOCKITEM WHERE IDENTIFIER = ? AND STOCKEXCHANGE = ?', (identifier, stockexchange,))

    if initialized and len(c.fetchall()) == 0: #if tables have been created and stock item doesn't exist already in db
        data = b.quote(identifier)

        change = NULL
        if CHANGE in data:
            change = validateDouble(data[CHANGE])

        faceValue = NULL
        if FACEVALUE in data:
            faceValue = validateDouble(data[FACEVALUE])

        fiftyTwoWeekHigh = NULL
        if FIFTYTWOWEEKHIGH in data:
            fiftyTwoWeekHigh = str(data[FIFTYTWOWEEKHIGH])

        fiftyTwoWeekLow = NULL
        if FIFTYTWOWEEKLOW in data:
            fiftyTwoWeekLow = str(data[FIFTYTWOWEEKLOW])

        stockPrice = NULL
        if STOCKPRICE in data:
            stockPrice = validateDouble(data[STOCKPRICE])

        marketCap = NULL
        if MARKETCAP in data:
            marketCap = validateDouble(data[MARKETCAP][VALUE])
            if NULL not in marketCap:
                if CRORE in data[MARKETCAP][IN]:
                    marketCap = float(marketCap) * CRORE_VAL
                if LAKH in data[MARKETCAP][IN]:
                    marketCap = float(marketCap) * LAKH_VAL
        marketCap = str(marketCap)

        stockName = NULL
        if STOCKNAME in data:
           stockName = data[STOCKNAME]

        fiveyryield = NULL
        currentyield = NULL

        if NULL not in stockPrice and NULL not in faceValue:
            list = handleDividend(identifier, stockPrice, faceValue)
            currentyield = str(list[0])
            fiveyryield = str(list[1])

        eps = NULL
        pe = NULL
        industry = NULL
        roe = NULL
        data = b.ratios(identifier)

        if PROFITRATIO in data: #attempt to get from ratios
            if PE in data[PROFITRATIO]:
                pe = str(data[PROFITRATIO][PE])
            if EPS in data[PROFITRATIO]:
                eps = str(data[PROFITRATIO][EPS])
        if VALUERATIO in data:
            if INDUSTRY in data[VALUERATIO]:
                industry = str (data[VALUERATIO][INDUSTRY])
            if ROE in data[VALUERATIO]:
               roe = str (data[VALUERATIO][ROE])

        data = b.peers(identifier)

        if NULL in eps and TABLE in data and EPS in data[TABLE][0]: #eps still null
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
        except Exception as e:
            print (str(e))

        c.execute('SELECT * FROM EXCHANGE WHERE EXCHANGENAME = ?', (stockexchange,))
        if len(c.fetchall()) != 0:
            #ensure table exists
            query = getQuery([identifier, stockexchange, change, faceValue, fiftyTwoWeekHigh, fiftyTwoWeekLow, stockPrice, marketCap, stockName, industry, eps, pe, currentyield, fiveyryield, roe])
            conn.execute(query);

def main():
    global companiesWithMissingInfo, missingFields
    companies = tqdm.tqdm(getBSECodes()) #get list of companies
    createTables() #create db tables
    for i in companies:
        insertBSEData(i)
        companies.set_description("Reading companies")

main()