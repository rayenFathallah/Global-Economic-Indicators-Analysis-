from src.components.data_transformation import * 
from src.logger import logging 
def transform(df1,df2,df3,name): 
    try : 
        new_df1=rename_columns(df1) 
        fill_missing_values(new_df1)
        new_df1=add_missing_countries(new_df1,df2,df3)
        new_df1=add_missing_dates(new_df1,df2,df3)
        formatted_df=reformat_dataframe(new_df1,name)
        format_date(formatted_df)
        logging.info('Data is transformed')
        return formatted_df
    except :
        logging.error('Transformation error') 
        raise CustomException('Error while transforming the data')

def transform_added_df(df,name): 
    try : 
        new_df=rename_columns(df)
        fill_missing_values(new_df)
        formatted_df=reformat_dataframe(new_df,name)
        format_date(formatted_df)
        logging.info('Data is transformed')
        return formatted_df
    except :
        logging.error('Transformation error') 
        raise CustomException('Error while transforming the data')
def transform_2df(df1,df2,name): 
    try : 
        new_df1=rename_columns(df1) 
        fill_missing_values(new_df1)
        new_df1=add_missing_countries_2df(new_df1,df2)
        new_df1=add_missing_dates_2df(new_df1,df2)
        formatted_df=reformat_dataframe(new_df1,name)
        format_date(formatted_df)
        logging.info('Data is transformed')
        return formatted_df
    except :
        logging.error('Transformation error') 
        raise CustomException('Error while transforming the data')