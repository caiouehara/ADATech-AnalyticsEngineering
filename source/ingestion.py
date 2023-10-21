import pandas as pd
from IPython.display import display
from sqlalchemy import create_engine 
from dotenv import load_dotenv
import os

load_dotenv()

def ingestion():
    #Extract
    df = pd.read_csv('/home/caiouehara/code-workspace/ADATech-AnalyticsEngineering/data/listings.csv')
    display(df)

    #Load bronze data
    conn_string = os.getenv("db_conn_string")
    try: 
        engine = create_engine(conn_string) 
        conn = engine.connect()
        df.to_sql('bronze_data', con=conn, if_exists='replace', 
        index=False)
        print("PSQL Updated")
    except Exception as e:
        raise e