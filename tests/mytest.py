from config.config_defs import CONFIG_PATH

# import modules
import sqlalchemy

# import objects
from databarge import SqlServerConnection, Etl

# define mandatory generic variables
config_params_path = CONFIG_PATH

# define optional generic variables
# log_path = r'xxx\log.log'

# make connections
source_connection = SqlServerConnection('MSSQLSVRA', config_params_path)
destination_connection = SqlServerConnection('MSSQLSVRB', config_params_path)

# define positional etl class variables
source_sql = r'''SELECT * FROM TESTDB.dbo.tbl_test'''
destination_database = 'TESTDB'
destination_table = 'tbl_test'

# define optional etl class variables
xforms = [
    "df['test_id'] = df['test_id'].astype(str)"
    , "df['test_value'] = df['test_quantity'] * df['test_rate']"
    , "df = df.drop(['test_quantity','test_rate'], axis = 1, inplace=True)"
    ]
dtypes = {'test_text':sqlalchemy.types.NVARCHAR(length=100)}
# destination_schema = 'someschema'

# create etl class
etl_1 = Etl(source_sql, destination_database, destination_table, source_connection, destination_connection
    , xforms = xforms
    , dtypes = dtypes
    # , destination_schema=destination_schema
    # , log_path=log_path
    )

# create other etl classes as required
    
# create a list of etl classes
xfers = [
    etl_1
    # , etl_2
    ]

# iterate through the etl list and execute the etl classes
for xf in xfers:
    
    # either drop or truncate the destination table
    xf.drop_table()
    # xf.truncate_table()
    
    # transfer the data
    xf.transfer_data()