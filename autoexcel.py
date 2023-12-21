import subprocess
import sys

try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
finally:
    import pandas as pd

try:
    import pandasql as pdsql
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandasql'])
finally:
    import pandasql as pdsql

try:
    import openpyxl
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'openpyxl'])

from pandas.api.types import is_datetime64_any_dtype
import numpy as np
import tomllib, os, re


with open('config.toml', 'rb') as file:
    cfg = tomllib.load(file)
    
    
files = os.listdir()
os.system('cls')

# Read both excel and csv files only, ignore temp files.
excelFiles = [f for f in files if re.search('XLSX', f, flags=re.I) and not re.search('~',f)]
csvFiles = [f for f in files if re.search('CSV', f, flags=re.I) and not re.search('~',f)]


# Lambda function for returning default in case key doesn't exist.
getOpt = lambda k, default: cfg['options'][k] if k in cfg['options'].keys() else default

# thousands and decimal delimiter for float values.
float_thousands_delimiter =  getOpt('float_thousands_delimiter', None)
float_decimal_point_delimiter =  getOpt('float_decimal_point_delimiter', '.')
# prefix for backup columns.
duplicate_prefix = '(old)' if getOpt('backup_modified_columns', False) else ''
# column header name to tell apart each file in the same table
path_column = getOpt('path_column', 'Source File')
# print file extension
print_file = getOpt('print_file', 'csv').lower()

# xlsx options
default_xlsx_header = getOpt('default_xlsx_header', None)
default_sheet_name = getOpt('default_sheet_name', 0)

# This will become the list of available dataframes (both xlsx and csv).
dataframes = list()

# Reading excels into the list of dataframes.
for xlsx in excelFiles:
    # Skip excluded files
    if any([file for file in cfg['options']['exclude'] if file in xlsx]):
        continue
        
    _df =\
    pd.read_excel(
        xlsx, # Excel file name.
        sheet_name = default_sheet_name,
        header = default_xlsx_header,
        decimal = float_decimal_point_delimiter,
        thousands = float_thousands_delimiter
    )
    
    dataframes.append((_df, xlsx))

    
# csv options.
default_csv_header = getOpt('default_csv_header', None) 
default_csv_delimiter = getOpt('default_csv_delimiter', None)

# Reading csv into the list of dataframes.
for csv in csvFiles:
    # Skip excluded files.
    if any([file for file in cfg['options']['exclude'] if file in csv]):
        continue
        
    _df =\
    pd.read_csv(
        csv, # Csv path.
        header = default_csv_header,
        delimiter = default_csv_delimiter,
        decimal = float_decimal_point_delimiter,
        thousands = float_thousands_delimiter
    )
    
    dataframes.append((_df, csv))
    
    
"""
This will cast the columns to the desired dtypes,
uses numpy dtypes so int8, uint64... are supported.
"""
processed_dataframes = list()

for df, path in dataframes:
    for col, dtype in cfg['columns'].items():
        # Raise exception if column doesn't exist:
        if not col in df.columns:
            raise Exception(f'!> Column {col} doesn\'t exist in table: {path}')
        
        if type(dtype) is dict:
            df[duplicate_prefix + col] = df[col]
            df[col] = pd.to_datetime(df[col], format=dtype['format'])
        else:
            df[duplicate_prefix + col] = df[col]
            df[col] = df[col].astype(np.dtype(dtype))
    
    # if path column name overrides an existing column, throw exception
    if path_column in df.columns:
        raise Exception(f'!> Table path column name \'{path_column}\' is duplicated, change it from the TOML file.')
    
    # Execute the query and save the dataframe
    # TODO: locals()
    filtered_df = pdsql.sqldf(cfg['query']['sql'], {'df': df})
    
    # This is to place path name first (reordering)
    filtered_df[path_column] = path
    remainingColumns = [c for c in filtered_df.columns if c != path_column]
    filtered_df = filtered_df[[path_column] + remainingColumns]
    processed_dataframes.append(filtered_df)
    
    
# Union of all the filtered dataframes with Source File column.
union_dataframe = pd.concat(processed_dataframes)


for col, dtype in cfg['columns'].items():
    if type(dtype) is not dict:
        continue
    union_dataframe[col] = pd.to_datetime(union_dataframe[col])
    # DTYPES SHOULD BE STRACTED HERE BEFORE CONVERSION
    union_dataframe[col] = union_dataframe[col].dt.date
    
    
# OUTPUT BEGINS HERE
outdir = './output'
if not os.path.exists(outdir):
    os.mkdir(outdir)
    

# Trying to export the file
name = 'dataframe'
extension = print_file


output_path = outdir + '/' + name + '.' + extension

print(3*'\n#')
print('# Output file:')

try:
    if extension == 'csv':
        union_dataframe.to_csv(output_path, sep=default_csv_delimiter, date_format='%Y-%m-%d', index=False)
        print(output_path)
    if extension == 'xlsx':
        union_dataframe.to_excel(output_path, index=False)
        print(output_path)
except:
    sub = lambda file: re.findall('dataframe\(?(\d*)\)?.' + extension, file)
    numbers = [sub(file) for file in os.listdir('./output') if len(sub(file)) > 0]
    copies = [int(number[0]) for number in numbers if number[0].isnumeric()]
    
    output_path = outdir + '/' + name + '(' + str(max([]) + 1 if [] else 0) + ').' + extension
    
    if extension == 'csv':
        union_dataframe.to_csv(output_path, sep=default_csv_delimiter, date_format='%Y-%m-%d', index=False)
        print(output_path)
    if extension == 'xlsx':
        union_dataframe.to_excel(output_path, index=False)
        print(output_path)