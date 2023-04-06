import pandas as pd
import argparse
import logging
from src.exception import CustomException


logging.basicConfig(level=logging.INFO)

def rename_columns(df):
    new_columns = []
    for column in df.columns:
        if '0' in column:
            new_column = column.replace('0', 'na')
            new_columns.append(new_column)
        else:
            new_columns.append(column)
    df.columns = new_columns

def fill_missing_values(df):
    df.fillna(0, inplace=True)

def add_missing_countries(df, reference_df):
    columns = set(df.columns)
    reference_columns = set(reference_df.columns)
    not_in_reference = list(columns - reference_columns)
    df[not_in_reference] = 0

def add_missing_dates(df, reference_df):
    dates = set(df['Year'].values)
    reference_dates = set(reference_df['Year'].values)
    dates_not_in_reference = list(dates - reference_dates)
    additional_dataframe = pd.DataFrame(columns=df.columns)
    additional_dataframe['Year'] = dates_not_in_reference
    columns = [col for col in df.columns if col != "Year"]
    additional_dataframe[columns] = 0
    df = pd.concat([additional_dataframe, df])
    return df

def reformat_dataframe(df, name):
    countries = [country for country in df if country != 'Year']
    formatted_df = df.melt(id_vars='Year', value_vars=countries, var_name='Country', value_name=name)
    formatted_df.rename(columns={'Year': 'Date'}, inplace=True)
    formatted_df['Date'] = pd.to_datetime(formatted_df['Date'], format='%YM%m')
    return formatted_df

def main(input_path, output_path):
    logging.info("Loading data...")
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process three file paths.')
    parser.add_argument('file1', type=str, help='Path to first file')
    parser.add_argument('file2', type=str, help='Path to second file')
    parser.add_argument('file3', type=str, help='Path to third file')

    try:
        # Parse arguments
        args = parser.parse_args()

        # Do something with the file paths
        file1_path = args.file1
        file2_path = args.file2
        file3_path = args.file3

        # Your code goes here to process the files

    except CustomException as e:
        # Catch CustomException and log error message
        logging.error(str(e))

    except Exception as e:
        # Catch any other exceptions and log error message
        logging.error("Unexpected error: " + str(e))
    cpi_data = pd.read_csv(input_path + 'CPI-monthly.csv')
    er_data = pd.read_csv(input_path + 'Exchange rate Monthly.csv')
    exports_data = pd.read_csv(input_path + 'Exports Merchandise Monthly.csv')
    
    logging.info("Cleaning and transforming data...")
    rename_columns