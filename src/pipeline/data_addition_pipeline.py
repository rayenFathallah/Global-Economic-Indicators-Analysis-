import sys 
from src.exception import CustomException 
from src.logger import logging 
from src.pipeline.transformation_pipeline import * 
import pandas as pd 
from src.components.add_data import * 
def add_data(input_path,output_path):
    CPI_Add_formatted=pd.DataFrame()
    ER_Add_formatted=pd.DataFrame()
    Exports_Add_formatted=pd.DataFrame()
    try : 
        CPI_data,ER_data,Exports_data=read_add_files(input_path)
        if(CPI_data.empty==False) : 
            CPI_Add_formatted=transform(CPI_data,ER_data,Exports_data)
            test_existance(CPI_Add_formatted)
        if(ER_data.empty==False) : 
            ER_Add_formatted=transform(ER_data,CPI_data,Exports_data)
            test_existance(ER_Add_formatted)
        if(Exports_data.empty==False) : 
            Exports_Add_formatted=transform(Exports_data,CPI_data,ER_data)
            test_existance(Exports_Add_formatted)        
    except :
        logging.error('Addition error error') 
        raise CustomException('Error while adding the data')