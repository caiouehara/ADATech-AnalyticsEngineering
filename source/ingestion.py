import pandas as pd
from IPython.display import display
from sqlalchemy import create_engine 
from dotenv import load_dotenv
import os

load_dotenv()

def ingestion():
    data = extractDataCSV('/home/caiouehara/code-workspace/ADATech-AnalyticsEngineering/rawdata/listings.csv')
    loadDataPSQL(data)
    return data

def extractDataCSV(path_csv):
    print("Extracting CSV data from: " + path_csv)
    data = pd.read_csv(path_csv)
    display(data)
    return data

def loadDataPSQL(data):
    conn_string = os.getenv("db_conn_string")
    print("Loading PSQL data to " + conn_string)
    
    try: 
        engine = create_engine(conn_string) 
        conn = engine.connect()
        data.to_sql('bronze_data', con=conn, if_exists='replace', 
        index=False)
        print("PSQL Updated")
        return data
    except Exception as e:
        raise e