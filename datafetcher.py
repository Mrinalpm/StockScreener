from bselib.bse import BSE
import requests
import tqdm
import json

#THIS IS USED ONLY FOR FETCHING BSE STOCKS DATA AND STORING IT IN A JSON FILE
#THE DATA IS SPECIFIC IN WHAT IT FETCHES AND IS MEANT TO BE USED WITH A STOCK SCREENING APPLICATION

# Global variables
# User defined variables

# link at which companies are available
stock_list_link = "https://s3.amazonaws.com/quandl-static-content/BSE%20Descriptions/stocks.txt"
file_name = "result.json"  #name of file in which data will be stored

KEYWORD = '|BOM'  # delimiter when reading company names
NULL = "NULL"
BSE_CODE_DIGITS = 6  # number of digits in a BSE stock code

QUOTE = "quote"
PERFORMANCE_ANALYSIS = "performance_analysis"
FINANCIAL_RATIOS = "financial_ratios"
PEER_COMPARISON = "peer_comparison"
CORPORATE_ACTIONS = "corporate_actions"
HOLDING_INFO_ANALYSIS = "shareholding_info_and_analysis"
# ----------------------------------------------------------------------------------------------------------------------#
b = BSE()  # instance of BSE

data_dict = dict()  #dictionary in which all data will be stored


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

        write_to_file() # clear contents
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
    global file_name, data_dict
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