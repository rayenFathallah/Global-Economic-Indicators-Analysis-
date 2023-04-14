from .connection import get_connection 
import pyodbc 
from src.exception import CustomException 
from src.logger import logging 
def create_db(df):
    try : 
        conn = get_connection()
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO Indicators_table (Date, Country, CPI, ER, Exports) VALUES (?, ?, ?, ?, ?)", row.Date, row.Country, row.CPI,row.ER,row.Exports)
        conn.commit()
        # Close the database connection
        cursor.close()



        logging.info("Database created successfully!")
    except CustomException as e:
        # Catch CustomException and log error message
        logging.error('Problem while connecting to the sql server database')


