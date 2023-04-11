import sys 
from src.exception import CustomException 
from src.logger import logging 
import pandas as pd 
def read_add_files(input_path):
    try : 
        CPI_data=pd.DataFrame()
        ER_data=pd.DataFrame() 
        Exports_data=pd.DataFrame()
        CPI_path = input("CPI data file to add ( 0 for none) ") 
        ER_path  = input("Exchange rate data file to add ( 0 for none) ") 
        Exports_path  = input("Exports data file to add ( 0 for none) ")
        if((CPI_path,ER_path,Exports_path)==('0','0','0')): 
            logging.error("No data files specified")
            sys.exit()
        if(CPI_path!='0'): 
            CPI_data = pd.read_csv(input_path+CPI_path)
        if(ER_path!='0'): 
            ER_data = pd.read_csv(input_path+ER_path)
        if(Exports_path!='0'): 
            Exports_data = pd.read_csv(input_path+Exports_path)
        logging.info('To-add data loadeded successfully! ')
        return CPI_data,ER_data,Exports_data 
    except CustomException as e:
        # Catch CustomException and log error message
        logging.error('A problem accured while reading the file')
def test_existance(df,added_df): 
# merge the dataframes based on 'date' and 'country' columns
    try : 
        merged = pd.merge(df, added_df, on=['Date', 'Country'], how='outer')

        # check for duplicates in the merged dataframe
        if merged.duplicated(subset=['date', 'country']).any():
            print("Duplicate rows found.")
            response = input("Do you want to add the values that are not present in both dataframes? (yes/no) ")
            if response.lower() == 'yes':
                result = pd.concat([df, added_df]).drop_duplicates(subset=['date', 'country'])
                print("Dataframes merged successfully.")
                logging.info("Dataframes merged successfully.")
            else:
                result = df
                print("Dataframes merged successfully.")
                logging.error("Added_data countains duplicates.")
        else:
            result = pd.concat([df, added_df]).drop_duplicates(subset=['date', 'country'])
            print("Dataframes merged successfully.") 
            logging.info("Dataframes merged successfully.")
        return result
    except CustomException as e:
        # Catch CustomException and log error message
        logging.error('A problem accured while merging the dataframes')
