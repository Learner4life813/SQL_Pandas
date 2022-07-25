import pyodbc
import pandas as pd
#import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import urllib

#The number of batches we want to divide the complete CUI list into
#row count per batch = sum of all row count / num of batches
number_of_batches = 5

pd.options.mode.chained_assignment = None
pyodbc.pooling = False

#DB parameters to read the T2C data
server = 'abcde'
database = 'Thesaurus'
query = '''SELECT cui, SUM([count]) AS CountOfRows 
            FROM reports.CUICounts
            WHERE RunServer = ''
            AND RunID = (SELECT max(RunID) FROM reports.CUICounts
            WHERE RunServer = '')
            GROUP BY cui
            ORDER BY CountOfRows desc'''
cnxn = pyodbc.connect('Driver=SQL Server;SERVER='+server+';DATABASE='+database+';Trusted_Connection=Yes')
with cnxn:
    cursor = cnxn.cursor()
    df = pd.read_sql(query, cnxn)

total_rows_to_be_changed = df['CountOfRows'].sum()
total_PHM_rows_per_batch = total_rows_to_be_changed/(number_of_batches-1)

#add a new column for BatchID
df['BatchID'] = pd.NaT

#assignment of Batch IDs
batch_id = 0
batch_sum = 0
for ind in df.index:
    if batch_id < number_of_batches:
        batch_sum += df['CountOfRows'][ind]
        if batch_sum > total_PHM_rows_per_batch or batch_id == 0:
            batch_id += 1
            batch_sum = df['CountOfRows'][ind]
        df['BatchID'][ind] = batch_id

#assign any unassigned rows to the last batch
df.loc[pd.isnull(df['BatchID']),'BatchID'] = batch_id

#Write the dataframe to table 'T2CBatch' in Scratch DB
database = 'Scratch'
connection_string = 'Driver=SQL Server;SERVER='+server+';DATABASE='+database+';Trusted_Connection=Yes'
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url, fast_executemany=True)

#SQL server permits 2100 parameters. This translates to 2100/(3 columns) = 700 rows ~500
df.to_sql(name='T2CBatch',con = engine,schema='dbo',index=False,method='multi',chunksize = 500, if_exists='replace')

#publish report - batches and total rows count per batch
df1 = df.groupby(['BatchID']).sum(['CountOfRows'])
print('Average row count per batch: ', int(total_rows_to_be_changed/number_of_batches))
print('-'*100)
print(df1)

#teardown
engine.dispose()
del df
