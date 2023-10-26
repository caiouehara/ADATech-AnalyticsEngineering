from IPython.display import display
# from ydata_profiling import ProfileReport

def cleanData(data):
    createDataProfile(data)
    transformedData = transformDataPipeline(data)
    display(transformedData)

def transformDataPipeline(data):
    #Pipeline of data transformation
    bucket_transform = [check_missing, fillNull]
    ndata = data
    for transform in bucket_transform:
        ndata = transform(ndata)
    
    return ndata

def createDataProfile(data):
    #json = data.describe().to_json()
    #json += data.dtypes().to_json()
    #writeJson(json)
    #profile = ProfileReport(data, title="Pandas Profiling Report") #cria o relatório
    #profile.to_file("resultados.html") #salva os resultados em um arquivo
    return data

def fillNull(df_cln):
    # inserir valores no dataset antes da análise, não pode atrapalhar a análise final?
    df_cln['notRepairedDamage'].value_counts()
    df_cln['notRepairedDamage'] = df_cln['notRepairedDamage'].fillna("no_info")
    df_cln['vehicleType'].value_counts()
    df_cln['vehicleType'] = df_cln['vehicleType'].fillna("no_info")
    df_cln['vehicleType'].value_counts()
    high_freq = df_cln['fuelType'].value_counts().idxmax()
    df_cln['fuelType'] = df_cln['fuelType'].fillna(high_freq) 
    check_missing(df_cln).sort_values(ascending=False)
    return df_cln

def check_missing(df_cln):
    res_missing = df_cln.isna().sum() 
    res_missing = (res_missing/len(df_cln))*100
    print(res_missing)
    return df_cln

def writeJson(jsonObject):
    with open("../resultados.json", "w") as outfile:
        outfile.write(jsonObject)