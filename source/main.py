from ingestion import ingestion
from cleanData import cleanData

def main():
    data = ingestion()
    cleanData(data)
    

main()