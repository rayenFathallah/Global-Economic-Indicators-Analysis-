import pyodbc

def get_connection():
    """
    Create a connection object for the SQL Server database.
    """
    
    server = 'DESKTOP-750KGBF' 
    database = 'International_indicators' 
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server}; \
        SERVER='+ server +'; \
        DATABASE='+ database +';\
        Trusted_Connection=yes;\
        TrustServerCertificate=yes;'
    )
    return cnxn