import os 
import sys 
from src.exception import CustomException 
from src.logger import logging 
import pandas as pd
import argparse


def read_files(input_path):
    logging.info("Loading data...")
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process three file paths.')
    parser.add_argument('CPI', type=str, help='Path to the CPI data file')
    parser.add_argument('Exchange_rate', type=str, help='Path to the exchange rate file')
    parser.add_argument('Exports', type=str, help='Path to exports file')

    try:
        # Parse arguments
        args = parser.parse_args()

        # Do something with the file paths
        CPI_path = args.CPI
        ER_path = args.Exchange_rate
        Exports_path = args.Exports


    except CustomException as e:
        # Catch CustomException and log error message
        logging.error(str(e))

    except Exception as e:
        # Catch any other exceptions and log error message
        logging.error("Error while reading the files: " + str(e))
    cpi_data = pd.read_csv(input_path + CPI_path+'.csv')
    er_data = pd.read_csv(input_path + ER_path+'.csv')
    exports_data = pd.read_csv(input_path +Exports_path+ '.csv')
    logging.info("Files read successfully! ")

    return (cpi_data,er_data,exports_data)
