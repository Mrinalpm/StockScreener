from bselib.bse import BSE
import sqlite3
import requests
import re
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


#identifier, stockexchange, change, fv, ftwh, ftwl, stockprice, marketcap, stockname, industry, eps, pe, divyield, fiveyearyield, roe
stockAttributes = [0] * 15

#----------------------------------------------------------------------------------------------------------------------------------------------------#
#Program variables
full_pattern = re.compile('1234567890.') #number pattern

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

def getLUTTable(list):
    toReturn = dict()

    for i in range(len(list)):
        toReturn[list[i]] = i

    return toReturn

def handleDividend(identifier, price, faceValue):
    data = b.corporate_actions(identifier)
    endOfFinYr = datetime.now()
    prevYearTotals = [0.0, 0.0, 0.0, 0.0, 0.0]

    if 'dividends' in data:
        if 'data' in data['dividends']:
            dictionary = getLUTTable(data['dividends']['header'])
            for i in data['dividends']['data']:
                date = getDate(i[dictionary["Record Date"]])

                if date <= endOfFinYr and date > endOfFinYr - relativedelta(years=1):
                    prevYearTotals[0] += float (i[dictionary['Dividend Percentage']].replace('%', ''))
                    #insert index 0
                elif date <= endOfFinYr - relativedelta(years=1) and date > endOfFinYr - relativedelta(years=2):
                    prevYearTotals[1] += float(i[dictionary['Dividend Percentage']].replace('%', ''))
                    #insert index 1
                elif date <= endOfFinYr - relativedelta(years=2) and date > endOfFinYr - relativedelta(years=3):
                    prevYearTotals[2] += float(i[dictionary['Dividend Percentage']].replace('%', ''))
                    #insert index 2
                elif date <= endOfFinYr - relativedelta(years=3) and date > endOfFinYr - relativedelta(years=4):
                    prevYearTotals[3] += float(i[dictionary['Dividend Percentage']].replace('%', ''))
                    # insert index 3
                elif date <= endOfFinYr - relativedelta(years=4) and date > endOfFinYr - relativedelta(years=5):
                    prevYearTotals[4] += float(i[dictionary['Dividend Percentage']].replace('%', ''))
                    # insert index 4

    fiveyryield = sum (prevYearTotals) * float(faceValue) / (5.0  * float(price)) #five year yield returned as a percentage
    currentyield = prevYearTotals[0] * float(faceValue) / float(price)

    return currentyield, fiveyryield

def getQuery(fields):
    global companiesWithMissingInfo

    missingInfo = False
    for i in range(len(fields)):
        if "NULL" in fields[i]:
            stockAttributes[i] += 1
            missingInfo = True

    if missingInfo:
        companiesWithMissingInfo += 1

    return "INSERT INTO STOCKITEM VALUES ('"+ fields[0] + "', '"+ fields[1] + "', "+ fields[2]+ ', '+ fields[3] + ', '+ fields[4] + ', '+ fields[5] + ', '+ fields[6] + ', '+ fields[7] + ", '"+ fields[8] + "', '"+ fields[9] + "', "+ fields[10]+ ', '+ fields[11] + ', '+ fields[12] + ', '+ fields[13] + ', '+ fields[14] + ');'

def insertBSEData(identifier):
    global initialized

    stockexchange = "BSE"
    c.execute('SELECT * FROM STOCKITEM WHERE IDENTIFIER = ? AND STOCKEXCHANGE = ?', (identifier, stockexchange,))

    if initialized and len(c.fetchall()) == 0:
        data = b.quote(identifier)

        change = "NULL"
        if "change" in data:
            change = validateDouble(data["change"])

        faceValue = "NULL"
        if "faceValue" in data:
            faceValue = validateDouble(data["faceValue"])

        fiftyTwoWeekHigh = "NULL"
        if 'fiftytwo_WeekHigh' in data:
            fiftyTwoWeekHigh = str(data['fiftytwo_WeekHigh'])

        fiftyTwoWeekLow = "NULL"
        if 'fiftytwo_WeekLow' in data:
            fiftyTwoWeekLow = str(data['fiftytwo_WeekLow'])

        stockPrice = "NULL"
        if "stockPrice" in data:
            stockPrice = validateDouble(data["stockPrice"])

        marketCap = "NULL"
        if 'mktCap' in data:
            marketCap = validateDouble(data['mktCap']['value'])
            if "NULL" not in marketCap:
                if CRORE in data['mktCap']['in']:
                    marketCap = float(marketCap) * 10000000
                if LAKH in data['mktCap']['in']:
                    marketCap = float(marketCap) * 100000
        marketCap = str(marketCap)

        stockName = 'NULL'
        if 'stockName' in data:
           stockName = data['stockName']

        fiveyryield = "NULL"
        currentyield = "NULL"

        if "NULL" not in stockPrice and "NULL" not in faceValue:
            list = handleDividend(identifier, stockPrice, faceValue)
            currentyield = str(list[0])
            fiveyryield = str(list[1])

        eps = "NULL"
        pe = "NULL"
        industry = "NULL"
        roe = "NULL"
        data = b.ratios(identifier)

        if 'profit_ratio' in data: #attempt to get from ratios
            if 'PE' in data['profit_ratio']:
                pe = str(data['profit_ratio']['PE'])
            if 'EPS' in data['profit_ratio']:
                eps = str(data['profit_ratio']['EPS'])
        if 'value_ratio' in data:
            if 'Industry' in data['value_ratio']:
                industry = str (data['value_ratio']['Industry'])
            if 'ROE' in data['value_ratio']:
               roe = str (data['value_ratio']['ROE'])

        data = b.peers(identifier)

        if "NULL" in eps and 'Table' in data and 'EPS' in data['Table'][0]: #eps still null
            if 'scrip_cd' in data['Table'][0] and data['Table'][0]['scrip_cd'] == float(identifier):
                eps = str(data['Table'][0]['EPS'])

        if "NULL" in pe and 'Table' in data and 'PE' in data['Table'][0]:
            if 'scrip_cd' in data['Table'][0] and data['Table'][0]['scrip_cd'] == float(identifier):
                pe = str(data['Table'][0]['PE'])

        try:
            c.execute('SELECT * FROM EXCHANGE WHERE EXCHANGENAME = ?', (stockexchange,))
            if len(c.fetchall()) == 0:
                #insert
                c.execute('INSERT INTO EXCHANGE VALUES (?, ?, ?)', ("BSE", "INDIA", "INR"))
        except Exception as e:
            print (str(e))

        c.execute('SELECT * FROM EXCHANGE WHERE EXCHANGENAME = ?', (stockexchange,))
        if len(c.fetchall()) != 0:
            #ensure table exists
            query = getQuery([identifier, stockexchange, change, faceValue, fiftyTwoWeekHigh, fiftyTwoWeekLow, stockPrice, marketCap, stockName, industry, eps, pe, currentyield, fiveyryield, roe])
            conn.execute(query);

def main():
    global companiesWithMissingInfo, missingFields
    companies = getBSECodes()
    createTables()
    for i in range(len(companies)):
        insertBSEData(companies[i])
        percent = round(float(i)*100.0 / len(companies))
        if i == len(companies) - 1 or (percent != 0 and percent % 10 == 0 and round(float(i+1)*100.0 / len(companies)) % 10 != 0):
            print("Completed " + str(percent) + "% (" + str(i + 1) + "/" + str(len(companies)) + ") companies read\nMissing Fields: " + str(missingFields) + "\nCompanies with missing info: " + str(companiesWithMissingInfo) + "\n\n")

main()