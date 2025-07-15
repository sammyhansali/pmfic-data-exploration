from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import polars as pl

def main():
    # Server, DB, and auth details
    server = 'neo'
    database = 'Logging'
    username = 'PROVMUTUAL/SHansali'
    trusted_connection = True

    # Pyodbc driver connection details
    driver = 'SQL Server'
    # driver = 'ODBC Driver 17 for SQL Server'

    # Setting up connection and engine
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};TRUSTED_CONNECTION={trusted_connection};'
    connection_url = URL.create(
        'mssql+pyodbc', 
        query={'odbc_connect': connection_string},
    )
    engine = create_engine(connection_url, use_setinputsizes = False, echo = False)

    # Creating a polars DataFrame from a SQL query
    query = '''
        SELECT TOP(10) *
        FROM Logging.Logs.Stored_Procedures
        '''
    df = pl.read_database(query, engine)
    print(df)

if __name__ == "__main__":
    main()
