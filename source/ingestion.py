import pandas as pd
from IPython.display import display

def ingestion():
    data = pd.read_csv('/home/caiouehara/code-workspace/ADATech-AnalyticsEngineering/data/listings.csv')
    display(data)
    