import imp
from re import X
import csv
from flask import request
import pandas_profiling
import pandas as pd
from timeit import default_timer as timer
import numpy as np
import pandas as pd
from math import isnan
from AutoClean import AutoClean
import hashlib




class profiling:
    
    
    def columnListFromPath(path):
           if (path.split(".")[1]== "csv") :
                df = pd.read_csv(path)
                dataColumn = df.columns
           elif(path.split(".")[1] == "xlsx"):
                df = pd.read_excel(path)
                dataColumn = df.columns
           return dataColumn

     
    def dataset_report(path):
            if (path.split(".")[1]== "csv") :
                df = pd.read_csv(r"datasets/"+path)
                filename=path.split(".")[0]
                data_profiling_report=pandas_profiling.ProfileReport(df, minimal=True , pool_size=4)
                data_profiling_report
                file= filename + ".html"
                print(file)
                data_profiling_report.to_file(r"static/templates/"+file)      
                return file
            elif(path.split(".")[1] == "xlsx"):
                df = pd.read_excel(r"datasets/"+path)
                filename=path.split(".")[0]
                data_profiling_report=pandas_profiling.ProfileReport(df, minimal=True , pool_size=4)
                data_profiling_report
                file= filename + ".html"
                print(file)
                data_profiling_report.to_file(r"static/templates/"+file)      
                return file
                
    def deleteDuplicate():
        data = pd.read_csv(r"datasets/olist_products_dataset.csv")
        dataColumns = data.columns
        lst=[]
        for x in dataColumns:
          lst.append(x)
        for elem in lst :
            data.drop_duplicates(subset=[str(elem)],keep="first",inplace=True)
        data.to_csv(r'new_datasets_without_duplicate/output.csv',index=True)

    def json_report(path):
            if (path.split(".")[1]== "csv") :
             df = pd.read_csv(r"datasets/"+path)
             filename=path.split(".")[0]
             data_profiling_report=pandas_profiling.ProfileReport(df, minimal=True , pool_size=4)
             data_profiling_report
             file= filename + ".json"
             print(file)
             data_profiling_report.to_file(r"templates/"+file)      
             return file


    def autoclean(path):
        if (path.split(".")[1]== "csv") :
            df = pd.read_csv(r"datasets/"+path)
            pipeline = AutoClean(df)
            pipelineOutput = pipeline.output
            newDataSet =  pipelineOutput.to_csv(r'new_datasets_autoclean/autocleanwithoutparams.csv')
            pathData = "C:/Users/Fares/Desktop/pfe/new_datasets_autoclean/autocleanwithoutparams.csv"

        return pathData
        
    def autocleanwithParams(path , **data ):  
        print("data",data)



        if (path.split(".")[1]== "csv") :
           df = pd.read_csv(r"datasets/"+path)
           pipeline = AutoClean(df, missing_num=data['missingNum'], missing_categ=data['missingCat'] ,outliers=data['outliers'] , outlier_param=data['outlierParam'] , extract_datetime=data['extractDateTime']  )
           # missing_categ=missingCat ,outliers=outliers , outlier_param=outlierParam , extract_datetime=extractDateTime 
           pipelineOutput = pipeline.output
           newDataSet =  pipelineOutput.to_csv(r'new_datasets_autoclean/autocleanwithparams.csv')
           pathData = "your new auto clean file path is located in : " + "C:/Users/Fares/Desktop/pfe/new_datasets/autocleanwithparams.csv"

        return pathData

    def hashColumn(path,listColumn):
     df = pd.read_csv(path)
     for x in listColumn:
          df[x] = df[x].astype(str)
          df[x] = df[x].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
     df.to_csv(r'new_datasets_hash_data/hash.csv')




    """
    def data(path):
        df = pd.read_csv(r"datasets/"+path)
    # Use Pandas to load the data file into a dataframe
        try:
            df = pd.read_csv(r"datasets/"+path)
        except:
            print("Error: Data file not found!")    
        return df
    def price_report(path):
        df = pd.read_csv(r"datasets/"+path)
        hourse_price_report=pandas_profiling.ProfileReport(df)
        hourse_price_report
        jsonFile=  hourse_price_report.to_json()     
        return jsonFile"""   