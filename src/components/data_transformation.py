import pandas as pd 
from src.exception import CustomException 
def rename_columns(df):
    try : 
    ### Some countries have 0 In place of na so it has to be corrected
        new_columns = []
        for column in df.columns:
            if '0' in column:
                new_column = column.replace('0', 'na')
                new_columns.append(new_column)
            else:
                new_columns.append(column)
        df.columns = new_columns
    except:
        raise CustomException('Error while renaming the countries in rename_columns() function')
    return df 
    

def fill_missing_values(df):
    try :  
    
        df.fillna(0, inplace=True)
    except:
        raise CustomException('Error while filling the missing values in fill_missing_values() function')



def add_missing_countries(df, reference_df1,reference_df2):
    
# Add missing countries for each dataframe 
    try : 
        reference_df1=rename_columns(reference_df1)
        reference_df2=rename_columns(reference_df2)
        df=rename_columns(df)
        columns = set(df.columns)
        reference1_columns = set(reference_df1.columns)
        reference2_columns = set(reference_df2.columns)
        not_in_df = list(reference1_columns.union(reference2_columns) - columns)
        df[not_in_df] = 0
    except:
        raise CustomException('Error while adding the missing countries in add_missing_countries() function')
    return df 
def add_missing_countries_2df(df, reference_df): 
    try : 
        reference_df=rename_columns(reference_df)
        df=rename_columns(df)   
        columns = set(df.columns)
        reference_columns = set(reference_df.columns)   
        not_in_df = list(reference_columns- columns)
        df[not_in_df] = 0
    except:
        raise CustomException('Error while adding the missing countries in add_missing_countries() function')
    return df 

def add_missing_dates(df, reference_df1,reference_df2):
# Add missing Dates for each dataframe
    try : 
        dates = set(df['Year'].values)
        if(reference_df1.empty==False): 
            reference1_dates = set(reference_df1['Year'].values)
        else : 
            reference1_dates=set()
        if(reference_df2.empty==False): 
            reference2_dates = set(reference_df2['Year'].values) 
        else : 
            reference2_dates=set()  
        dates_not_in_df = list(reference1_dates.union(reference2_dates) - dates)
        additional_dataframe = pd.DataFrame(columns=df.columns)
        additional_dataframe['Year'] = dates_not_in_df
        columns = [col for col in df.columns if col != "Year"]
        additional_dataframe[columns] = 0
        df = pd.concat([additional_dataframe, df])
        return df
    except : 
        raise CustomException('Error while adding the missing dates in add_missing_dates() function')
def add_missing_dates_2df(df,reference_df): 
    try : 
        dates = set(df['Year'].values)
        if(reference_df.empty==False): 
            reference_dates = set(reference_df['Year'].values)
        else : 
            reference_dates=set()
        additional_dataframe = pd.DataFrame(columns=df.columns)
        dates_not_in_df = list(reference_dates- dates)
        additional_dataframe['Year'] = dates_not_in_df
        columns = [col for col in df.columns if col != "Year"]
        additional_dataframe[columns] = 0
        df = pd.concat([additional_dataframe, df])
        return df
    except : 
        raise CustomException('Error while adding the missing dates in add_missing_dates() function')
def add_missing_dates2(df, *reference_dfs):
    """
    Add missing dates for each dataframe.

    Parameters:
    df (pandas.DataFrame): the dataframe to which missing dates should be added
    *reference_dfs (pandas.DataFrame): one or more dataframes used as reference for missing dates

    Returns:
    pandas.DataFrame: the original dataframe with missing dates added
    """
    try:
        dates = set(df['Year'].values)
        reference_dates = set().union(*[set(df['Year'].values) for df in reference_dfs if not df.empty])
        dates_not_in_df = list(reference_dates - dates)
        if dates_not_in_df:
            additional_dataframe = pd.DataFrame(0, columns=df.columns, index=range(len(dates_not_in_df)))
            additional_dataframe['Year'] = dates_not_in_df
            df = pd.concat([additional_dataframe, df])
        return df
    except:
        raise CustomException('Error while adding the missing dates in add_missing_dates() function')

def reformat_dataframe(df, name):
# Change the format of the data from country names as columns to values 
    try : 
        countries = [country for country in df if country != 'Year']
        formatted_df = df.melt(id_vars='Year', value_vars=countries, var_name='Country', value_name=name)
        formatted_df.rename(columns={'Year': 'Date'}, inplace=True)
        formatted_df['Date'] = pd.to_datetime(formatted_df['Date'], format='%YM%m')
        return formatted_df
    except:
        raise CustomException('Error while reformatting the dataframes in reformat_dataframe() function')


def format_date(dataframe):
    # Rename the 'Year' column to 'Date'
    try : 
        dataframe.rename(columns={'Year': 'Date'}, inplace=True)
        
        # Convert the 'Date' column to a pandas DateTime object
        dataframe['Date'] = pd.to_datetime(dataframe['Date'], format='%YM%m')
    except:
        raise CustomException('Error while reformatting the dates in format_date() function')

