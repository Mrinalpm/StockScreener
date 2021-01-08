from bselib.bse import BSE
import requests
import tqdm
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import xlsxwriter
from variables import *

#Script for generating a csv file of BSE stocks.
#Can also be used to scrape stock data from various sources (refer to bselib)
# ---------------------------------------------------------------------------------------------------------------------#

data_dict = dict()  #dictionary in which all data will be stored

"""
Function for getting 6 digit codes from the BSE
Returns list containing codes
"""
def get_bse_codes():
    to_return = []  # list to be returned
    try:
        data = requests.get(stock_list_link).text.splitlines()  # get data as array of lines
        for i in data:
            if KEYWORD in i:
                to_add = i.split(KEYWORD)[1]  # the 6-digit code
                if len(to_add) == BSE_CODE_DIGITS:
                    to_return.append(to_add)
    except Exception:
        #read from backup file
        f = open(stock_list_file, "r")
        data = f.read().splitlines()
        for i in data:
            if KEYWORD in i:
                to_add = i.split(KEYWORD)[1]  # the 6-digit code
                if len(to_add) == BSE_CODE_DIGITS:
                    to_return.append(to_add)
    return to_return

"""
Fetches relevant data of companies and stores in json file.
Can take up to 24 hrs
NOTE: THIS WILL ERASE DATA ALREADY PRESENT IN EXISTING JSON FILE
"""
def store_data():
    global data_dict

    companies = get_bse_codes()  # get list of companies

    progressbar = tqdm.tqdm(companies)

    count = 0

    try:

        write_to_file() # clear contents (write an empty dict)
        b = BSE()
        for i in progressbar:
            local_dict = dict()

            if i not in data_dict: #fetch data only if company isn't present
                local_dict[QUOTE] = b.quote(i)

                local_dict[PERFORMANCE_ANALYSIS] = b.analysis(i)

                local_dict[FINANCIAL_RATIOS] = b.ratios(i)

                local_dict[PEER_COMPARISON] = b.peers(i)

                local_dict[CORPORATE_ACTIONS] = b.corporate_actions(i)

                local_dict[HOLDING_INFO_ANALYSIS] = b.holdings(i)

                data_dict[i] = local_dict #if all is well, write to main dict

                count += 1

                if count % 10 == 0:  #periodically write to file
                    write_to_file()
                    count = 0
                progressbar.set_description("Reading companies")

    except Exception as e:
        print(str(e))


"""
Write json company data to file
"""
def write_to_file():
    global data_dict

    # Open a file: file
    file = open(file_name, mode='r')

    # read all lines at once
    all_of_it = file.read()

    # close the file
    file.close()

    open(file_name, 'w').close() #clear
    try:
        with open(file_name, 'w') as fp:
            json.dump(data_dict, fp, allow_nan=True)
        fp.close()
    except Exception: #something went wrong, restore old data
        with open(file_name, 'w') as fp:
            fp.write(all_of_it)
        fp.close()

"""
Function for validating a float as a string
Returns NULL if invalid
"""
def validate_double(str_param):

    str_param = str(str_param)
    # noinspection PyBroadException
    try:
        to_return = str(float(str_param.replace(',', '')))  # if any commas exist, remove them
    except Exception:  # exception occured, just return NULL
        to_return = "NULL"
    return to_return

def get_date(date):
    return datetime.strptime(date, date_format)

"""
Function to get current yield and 5 year average yield
"""
def handle_dividend(price, face_value, div_data):
    end_of_fin_yr = datetime.now()

    # each of the previous 5 year slabs. This will contain dividend percentages
    prev_yr_totals = [0.0, 0.0, 0.0, 0.0, 0.0]

    #Values to be returned
    fiveyryield = NULL
    currentyield = NULL

    if CORPORATE_ACTIONS_DIVIDENDS in div_data:
        if CORPORATE_ACTIONS_DIVIDENDS_DATA in div_data[CORPORATE_ACTIONS_DIVIDENDS]:
            dictionary = get_lookup_table(div_data[CORPORATE_ACTIONS_DIVIDENDS][CORPORATE_ACTIONS_DIVIDENDS_HEADER])  # get the lookup table
            for i in div_data[CORPORATE_ACTIONS_DIVIDENDS][CORPORATE_ACTIONS_DIVIDENDS_DATA]:
                date = get_date(i[dictionary[CORPORATE_ACTIONS_DIVIDENDS_RECORD_DATE]])  # date of dividend issued

                if end_of_fin_yr - relativedelta(years=1) < date <= end_of_fin_yr:
                    prev_yr_totals[0] += float(i[dictionary[CORPORATE_ACTIONS_DIVIDENDS_DIVIDEND_PERCENTAGE]].replace('%', ''))
                    # dividend issued current year
                elif end_of_fin_yr - relativedelta(years=2) < date <= end_of_fin_yr - relativedelta(years=1):
                    prev_yr_totals[1] += float(i[dictionary[CORPORATE_ACTIONS_DIVIDENDS_DIVIDEND_PERCENTAGE]].replace('%', ''))
                    # dividend issued previous year
                elif end_of_fin_yr - relativedelta(years=3) < date <= end_of_fin_yr - relativedelta(years=2):
                    prev_yr_totals[2] += float(i[dictionary[CORPORATE_ACTIONS_DIVIDENDS_DIVIDEND_PERCENTAGE]].replace('%', ''))
                    # dividend issued two years ago
                elif end_of_fin_yr - relativedelta(years=4) < date <= end_of_fin_yr - relativedelta(years=3):
                    prev_yr_totals[3] += float(i[dictionary[CORPORATE_ACTIONS_DIVIDENDS_DIVIDEND_PERCENTAGE]].replace('%', ''))
                    # dividend issued three years ago
                elif end_of_fin_yr - relativedelta(years=5) < date <= end_of_fin_yr - relativedelta(years=4):
                    prev_yr_totals[4] += float(i[dictionary[CORPORATE_ACTIONS_DIVIDENDS_DIVIDEND_PERCENTAGE]].replace('%', ''))
                    # dividend issued four years ago

            # five year yield returned as a percentage
            fiveyryield = sum(prev_yr_totals) * float(face_value) / (5.0 * float(price))
            currentyield = prev_yr_totals[0] * float(face_value) / float(price)  # current yield returned as a percentage

    return currentyield, fiveyryield

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
Function that will read data from json file and into the dictionary
"""
def init():
    global data_dict
    try:
        with open(file_name) as json_file:
            data_dict = json.load(json_file)
    except Exception as e:
        print(str(e))

"""
Function for creating excel file from given data
"""
def get_data():

    workbook = xlsxwriter.Workbook(excel_file_name)
    worksheet = workbook.add_worksheet()

    for col_num, dat in enumerate(headers):
        worksheet.write(0, col_num, dat)

    count = 1
    for i in data_dict:
        try:
            myArray = [i]
            added = False

            #Stock Name
            if QUOTE_STOCK_NAME in data_dict[i][QUOTE]:
                if data_dict[i][QUOTE][QUOTE_STOCK_NAME]:
                    myArray.append(str(data_dict[i][QUOTE][QUOTE_STOCK_NAME]))
                    added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            # Stock Price
            stockPrice = NULL
            if QUOTE_STOCK_PRICE in data_dict[i][QUOTE]:
                stockPrice = validate_double(data_dict[i][QUOTE][QUOTE_STOCK_PRICE])
                if NULL in stockPrice:
                    myArray.append(stockPrice)
                else:
                    myArray.append(float(stockPrice))
                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #Percent Change
            if QUOTE_PERCENT_CHANGE in data_dict[i][QUOTE]:
                percentC = validate_double(data_dict[i][QUOTE][QUOTE_PERCENT_CHANGE])
                if NULL in percentC:
                    myArray.append(percentC)
                else:
                    myArray.append(float(percentC))
                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #52 high
            if QUOTE_FIFTY_HIGH in data_dict[i][QUOTE]:
                fh = validate_double(data_dict[i][QUOTE][QUOTE_FIFTY_HIGH])
                if NULL in fh:
                    myArray.append(fh)
                else:
                    myArray.append(float(fh))

                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #52 low
            if QUOTE_FIFTY_LOW in data_dict[i][QUOTE]:
                fl = validate_double(data_dict[i][QUOTE][QUOTE_FIFTY_LOW])
                if NULL in fl:
                    myArray.append(fl)
                else:
                    myArray.append(float(fl))
                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            # Month High Low
            if QUOTE_MONTH_HIGH_LOW in data_dict[i][QUOTE]:
                myArray.append(str(data_dict[i][QUOTE][QUOTE_MONTH_HIGH_LOW]))
                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #Face Value
            faceValue = NULL
            if QUOTE_FACE_VALUE in data_dict[i][QUOTE]:
                faceValue = validate_double(data_dict[i][QUOTE][QUOTE_FACE_VALUE])

                if NULL in faceValue:
                    myArray.append(faceValue)
                else:
                    myArray.append(float(faceValue))
                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #Market Cap
            if QUOTE_MARKET_CAP in data_dict[i][QUOTE]:
                if QUOTE_MARKET_CAP_IN in data_dict[i][QUOTE][QUOTE_MARKET_CAP] and QUOTE_MARKET_CAP_VALUE in data_dict[i][QUOTE][QUOTE_MARKET_CAP]:
                    value = validate_double(data_dict[i][QUOTE][QUOTE_MARKET_CAP][QUOTE_MARKET_CAP_VALUE])
                    if not NULL in value:
                        value = float(value) * number_suffix[data_dict[i][QUOTE][QUOTE_MARKET_CAP][QUOTE_MARKET_CAP_IN]]
                        myArray.append(float(str(value)))
                    else:
                        myArray.append(str(value))
                    added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #PE

            if FINANCIAL_RATIOS_VALUE_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                if FINANCIAL_RATIOS_PE in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO]:
                    value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO][FINANCIAL_RATIOS_PE])
                    if not NULL in value:
                        myArray.append(float(str(value)))
                        added = True

            if not added:
                if FINANCIAL_RATIOS_PROFIT_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                    if FINANCIAL_RATIOS_PE in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_PROFIT_RATIO]:
                        value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_PROFIT_RATIO][FINANCIAL_RATIOS_PE])
                        if not NULL in value:
                            myArray.append(float(str(value)))
                            added = True

            if not added:
                if PEER_COMPARISON_TABLE in data_dict[i][PEER_COMPARISON]:
                    for temp in data_dict[i][PEER_COMPARISON][PEER_COMPARISON_TABLE]:
                        if PEER_COMPARISON_SCRIP_CD in temp:
                            if str(temp[PEER_COMPARISON_SCRIP_CD]) == i and PEER_COMPARISON_PE in temp:
                                value = (str(validate_double(temp[PEER_COMPARISON_PE])))
                                if NULL in value:
                                    myArray.append(value)
                                else:
                                    myArray.append(float(value))
                                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #EPS

            if FINANCIAL_RATIOS_VALUE_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                if FINANCIAL_RATIOS_EPS in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO]:
                    value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO][FINANCIAL_RATIOS_EPS])
                    if not NULL in value:
                        myArray.append(float((str(value))))
                        added = True

            if not added:
                if FINANCIAL_RATIOS_PROFIT_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                    if FINANCIAL_RATIOS_EPS in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_PROFIT_RATIO]:
                        value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_PROFIT_RATIO][FINANCIAL_RATIOS_EPS])
                        if not NULL in value:
                            myArray.append(float(str(value)))
                            added = True

            if not added:
                if PEER_COMPARISON_TABLE in data_dict[i][PEER_COMPARISON]:
                    for temp in data_dict[i][PEER_COMPARISON][PEER_COMPARISON_TABLE]:
                        if PEER_COMPARISON_SCRIP_CD in temp:
                            if str(temp[PEER_COMPARISON_SCRIP_CD]) == i and PEER_COMPARISON_EPS in temp:
                                value = (str(validate_double(temp[PEER_COMPARISON_EPS])))
                                if NULL in value:
                                    myArray.append(value)
                                else:
                                    myArray.append(float(value))
                                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #CEPS

            if FINANCIAL_RATIOS_VALUE_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                if FINANCIAL_RATIOS_CEPS in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO]:
                    value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO][FINANCIAL_RATIOS_CEPS])
                    if not NULL in value:
                        myArray.append(float(str(value)))
                        added = True

            if not added:
                if FINANCIAL_RATIOS_PROFIT_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                    if FINANCIAL_RATIOS_CEPS in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_PROFIT_RATIO]:
                        value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_PROFIT_RATIO][FINANCIAL_RATIOS_CEPS])
                        if not NULL in value:
                            myArray.append(float(str(value)))
                            added = True

            if not added:
                if PEER_COMPARISON_TABLE in data_dict[i][PEER_COMPARISON]:
                    for temp in data_dict[i][PEER_COMPARISON][PEER_COMPARISON_TABLE]:
                        if PEER_COMPARISON_SCRIP_CD in temp:
                            if str(temp[PEER_COMPARISON_SCRIP_CD]) == i and PEER_COMPARISON_CEPS in temp:
                                value = (str(validate_double(temp[PEER_COMPARISON_CEPS])))
                                if NULL in value:
                                    myArray.append(value)
                                else:
                                    myArray.append(float(value))
                                added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            #ROE

            if FINANCIAL_RATIOS_VALUE_RATIO in data_dict[i][FINANCIAL_RATIOS]:
                if FINANCIAL_RATIOS_ROE in data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO]:
                    value = validate_double(data_dict[i][FINANCIAL_RATIOS][FINANCIAL_RATIOS_VALUE_RATIO][FINANCIAL_RATIOS_ROE])
                    if not NULL in value:
                        myArray.append(float(str(value)))
                        added = True

            if not added:
                myArray.append('')
            added = False

            #--------------------------------------------------------------------------------------------------------------#
            # Dividend Yield

            if not NULL in faceValue and not NULL in stockPrice:
                array = handle_dividend(stockPrice, faceValue, data_dict[i][CORPORATE_ACTIONS])
                if NULL in str(array[0]):
                    myArray.append(array[0])
                else:
                    myArray.append(float(array[0]))
                if NULL in str(array[1]):
                    myArray.append(array[1])
                else:
                    myArray.append(float(array[1]))

            # --------------------------------------------------------------------------------------------------------------#

            temp = data_dict[i][HOLDING_INFO_ANALYSIS]
            if HOLDING_INFO_ANALYSIS_HOLDINGS in temp:

                if HOLDING_INFO_ANALYSIS_DATA in temp[HOLDING_INFO_ANALYSIS_HOLDINGS]:
                    if HOLDING_INFO_ANALYSIS_PIE in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA]:
                        if HOLDING_INFO_ANALYSIS_MUTUAL_FUNDS in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_MUTUAL_FUNDS]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_MUTUAL_FUNDS][HOLDING_INFO_ANALYSIS_PERC].replace('%',''))))

                        if HOLDING_INFO_ANALYSIS_INSURANCE in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_INSURANCE]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][ HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_INSURANCE][HOLDING_INFO_ANALYSIS_PERC].replace('%',''))))

                        if HOLDING_INFO_ANALYSIS_FII in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_FII]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_FII][HOLDING_INFO_ANALYSIS_PERC].replace('%',''))))

                        if HOLDING_INFO_ANALYSIS_PROMOTERS in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_PROMOTERS]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_PROMOTERS][HOLDING_INFO_ANALYSIS_PERC].replace('%',''))))

                        if HOLDING_INFO_ANALYSIS_OTHER in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_OTHER]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_OTHER][HOLDING_INFO_ANALYSIS_PERC].replace('%', ''))))

                        if HOLDING_INFO_ANALYSIS_NON_INSTITUTION in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_NON_INSTITUTION]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_NON_INSTITUTION][HOLDING_INFO_ANALYSIS_PERC].replace('%', ''))))

                        if HOLDING_INFO_ANALYSIS_OTHER_DIIS in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][ HOLDING_INFO_ANALYSIS_PIE]:
                            if HOLDING_INFO_ANALYSIS_PERC in temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_OTHER_DIIS]:
                                myArray.append(float(validate_double(temp[HOLDING_INFO_ANALYSIS_HOLDINGS][HOLDING_INFO_ANALYSIS_DATA][HOLDING_INFO_ANALYSIS_PIE][HOLDING_INFO_ANALYSIS_OTHER_DIIS][HOLDING_INFO_ANALYSIS_PERC].replace('%', ''))))

            for col_num, dat in enumerate(myArray):
                worksheet.write(count, col_num, dat)

            count += 1

        except Exception as e:
            print(str(e) + " " + i)
            workbook.close()

    workbook.close()

#CALL store_data ONLY IF LATEST DATA IS REQUIRED. THIS PROCESS CAN TAKE UP TO 24 HOURS
#store_data()
init()
get_data()