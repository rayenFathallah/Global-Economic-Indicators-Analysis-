import pandas as pd
from src.exception import CustomException
from src.logger import logging 
from src.components.data_ingestion import read_files
from src.pipeline.transformation_pipeline import transform , transform_added_df , transform_2df 
from src.sql_data_conn.queries import create_db
from src.sql_data_conn.queries import data_update
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
#main('./Monthly_data/','./Transformed_data/')
def update(input_path): 
    update_type = int(input('Will you update the three indicators or one ? : 1 for only one, 2 for two , 3 for 3 ')) 
    if(update_type==1): 
        name = input('Please enter the name of the indicator ( CPI - ER - EXPORTS)') 
        file_path = input('Please enter the file path')
        df = pd.read_csv(input_path+file_path) 
        formatted_df = transform_added_df(df,name)
        formatted_df.to_csv('Transformed_df.csv')
        data_update(formatted_df,[name])
    elif(update_type==2):
        name1 = input('Please enter the name of the first indicator ( CPI - ER - EXPORTS)')
        name2=  input('Please enter the name of the second indicator ( CPI - ER - EXPORTS)')
        file_path1 = input('Please enter the file path for the first indicator')
        file_path2 = input('Please enter the file path for the second indicator')
        df1 = pd.read_csv(input_path+file_path1)
        df2 = pd.read_csv(input_path+file_path2) 
        formatted_df1 = transform_2df(df1,df2,name1)
        formatted_df2 = transform_2df(df2,df1,name2)
        merged = pd.merge(formatted_df1,formatted_df2, on=['Date', 'Country'], how='outer')
        merged.to_csv('Transformed2df.csv')
        data_update(merged,[name1,name2])
    else : 
        CPI_data,ER_data,Exports_data=read_files(input_path) 
        formatted_ER = transform(ER_data,CPI_data,Exports_data,'ER')
        formatted_CPI = transform(CPI_data,ER_data,Exports_data,'CPI')
        formatted_Exports = transform(Exports_data,CPI_data,Exports_data,'Exports')
        merged = pd.merge(formatted_CPI, formatted_ER, on=['Date', 'Country'], how='outer')
        merged = pd.merge(merged, formatted_Exports, on=['Date', 'Country'], how='outer')
        data_update(merged,['CPI','ER','Exports'])
        logging.info('Process is done')
update('./Monthly_data/') 


