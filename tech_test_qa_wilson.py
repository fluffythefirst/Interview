import pandas as pd
from sqlalchemy import create_engine, text
import sys
"""
    1. There are a lot of trades that have their close time more than 30 days after the open time. More than 5,000 records have their close time in 2022
    and open time in 2020. This problem has been addressed in the SQL query, but the correct close time cannot be determined by the given infomation.
    2. Some contract size is shown as Null in the trade table. It seems to only related with symbol 'COFFEE'    
    3. Pandas is used in this case due to the data volume
    
"""
def check_Alphanumeric(df):
    """Check if the column contains non-alphanumeric values"""
    columns_to_check = ['login_hash','ticket_hash','server_hash','symbol','digits','cmd']
    non_alphanum = df[df[columns_to_check].map(lambda x: bool(pd.notna(x) and not str(x).isalnum())).any(axis=1)]
    return non_alphanum

def check_numeric_column(df):
    """Check if the column has only numeric values."""
    columns_to_check = ['digits','cmd','volume','open_price','contractsize']
    non_numeric = df[~df[columns_to_check].map(lambda x: isinstance(x, (int, float))).any(axis=1)]
    return non_numeric

def server_integrity(trades,users):
    """Check if any server hash only exists in the trades or users table"""
    trades_server = set(trades['server_hash'].unique())
    users_server = set(users['server_hash'].unique())
    only_in_trade = trades_server.difference(users_server)
    only_in_user = users_server.difference(trades_server)
    return only_in_trade, only_in_user


def login_only_in_trade(trades,users):
    """Check if any login hashes only appear in the trades table"""
    trades_login = set(trades['login_hash'].unique())
    users_login = set(users['login_hash'].unique())
    only_in_trade_login = trades_login.difference(users_login)
    return only_in_trade_login

def invalid_open_time(trades):
    """Check if any records have their open time after close time"""
    invalid_open_time_record = trades[trades['open_time'] >= trades['close_time']]
    return invalid_open_time_record

def invalid_close_time(trades, max_day_difference):
    """Check if the close time of trade is n days after the open time"""
    delta = pd.Timedelta(days = max_day_difference)
    invalid_close_time_record = trades[trades['close_time'] - trades['open_time'] >= delta]
    return invalid_close_time_record

def fetch_data(host,port,username,password,dbname):
    db_config = {
        "username": username,
        "password": password,
        "host": host,
        "port": port,
        "dbname": dbname
    }
    db_url = f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            trades = pd.read_sql(text('SELECT * FROM Trades'), conn)
            users = pd.read_sql(text('SELECT * FROM Users'), conn)
            print('Data fetch succeeded')
    except Exception as e:
        print("Connection failed:", e)
        trades, users = None, None

    return trades, users


if __name__ == "__main__":
    host = sys.argv[1] 
    port = sys.argv[2]   
    username = sys.argv[3] 
    password = sys.argv[4]       
    dbname = sys.argv[5] 
    max_day_difference = 30

    trades,users = fetch_data(host,port,username,password,dbname)
    non_alphanum = check_Alphanumeric(trades)
    non_numeric = check_numeric_column(trades)
    only_in_trade, only_in_user = server_integrity(trades,users)
    only_in_trade_login = login_only_in_trade(trades,users)
    invalid_open_time_record = invalid_open_time(trades)
    invalid_close_time_record = invalid_close_time(trades, max_day_difference)
    print(f'There are {non_alphanum.shape[0]} rows that contain non-alphanumeric values')
    print(f'There are {non_numeric.shape[0]} rows that contain non_numeric values')
    print('The following server hash only found in trades', *only_in_trade)
    print('The following server hash only found in users', *only_in_user)
    print(f'There are {len(only_in_trade_login)} login that can only be found in Trades table')
    print(f'There are {invalid_open_time_record.shape[0]} records that have open time greater than or equal to close time')
    print(f'There are {invalid_close_time_record.shape[0]} records that have close time greater than open time by {max_day_difference} days')