import pandas as pd
from src.exception import CustomException
from src.logger import logging 
from src.components.data_ingestion import read_files
from src.pipeline.transformation_pipeline import transform
from src.sql_data_conn.queries import create_db
logging.info('Setting up')
def main(input_path,output_path):    
    CPI_data,ER_data,Exports_data=read_files(input_path)
    formatted_ER = transform(ER_data,CPI_data,Exports_data,'ER')
    formatted_CPI = transform(CPI_data,ER_data,Exports_data,'CPI')
    formatted_Exports = transform(Exports_data,CPI_data,Exports_data,'Exports')
    merged = pd.merge(formatted_CPI, formatted_ER, on=['Date', 'Country'], how='outer')
    merged = pd.merge(merged, formatted_Exports, on=['Date', 'Country'], how='outer')
    logging.info('Process is done')
    ###merged.to_csv(output_path+'transformed_data.csv',index=False)
    create_db(merged)
main('./Monthly_data/','./Transformed_data/')
