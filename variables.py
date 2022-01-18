"""
Block 1 variables
"""
# It is assumed that the first row contains a header

#The name of the file containing BSE 6-digit codes of active equities
active_equities_file_name = "Equity.csv"

#The column number containing the BSE codes
column_number_containing_bse_codes = 1

#----------------------------------------------------------------------------------------------------------
"""
Block 2 variables
"""

#The following are "keys" that appear within a dictionary when company info is requested from bselib. Refer to documentation for more info
QUOTE = "quote"
PERFORMANCE_ANALYSIS = "performance_analysis"
FINANCIAL_RATIOS = "financial_ratios"
PEER_COMPARISON = "peer_comparison"
CORPORATE_ACTIONS = "corporate_actions"
HOLDING_INFO_ANALYSIS = "shareholding_info_and_analysis"

#Name of the .json file that will contain all company data
json_file_containing_data = "result.json"

#Company data is stored in a local buffer as it is read. This frequency specifies how often this buffer must be written to "json_file_containing_data"
company_data_write_frequency = 10

#----------------------------------------------------------------------------------------------------------
"""
Block 3 variables
"""

#Name of the soreadsheet file within which company data will be written
SPREADSHEET_FILE_NAME = "data.xlsx"