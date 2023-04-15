from .connection import get_connection 
from src.pipeline.transformation_pipeline import transform_added_df
import pyodbc 
from src.exception import CustomException 
from src.logger import logging 
import pandas as pd 
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


def data_update(df,names): 
    try: 
        sql = f"""
        MERGE Indicators_table AS target
        USING (
            SELECT Date, Country, {', '.join(names)} FROM ?
        ) AS source
        ON target.Date = source.Date AND target.Country = source.Country
        WHEN MATCHED {', '.join([f"AND target.{i} = 0 THEN UPDATE SET target.{i} = source.{i}" for i in names])}
        WHEN NOT MATCHED THEN
            INSERT (Date, Country, {', '.join(names)}) VALUES (source.Date, source.Country, {', '.join([f"source.{i}" for i in names])});
        """
        conn=get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, (df,))
        conn.commit()
        conn.close()
    except :
        logging.error('Error while updating the database') 
        raise CustomException('Error while updating the database')
