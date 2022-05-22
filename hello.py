

from importlib.resources import path
from flask import Flask, jsonify, render_template, request
import flask
from flask_cors import CORS
from pydantic import Json
from werkzeug.utils import secure_filename
from werkzeug.datastructures import  FileStorage
import os
from dataprofiling import profiling
from dataprofiling import profiling
from AutoClean import AutoClean
import glob


from flask_pymongo import PyMongo
import pymongo
import pandas as pd
from datetime import datetime
from re import X
import pandas_profiling
import pandas as pd
from timeit import default_timer as timer
import numpy as np
import pandas as pd
from math import isnan



myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["DQ_Analyzer"]
mycollection = mydb["datasets_path"]
mycollection2 = mydb["new_output_data"]





UPLOAD_FOLDER = 'datasets/'

app = Flask(__name__)
with app.test_request_context():
      print (flask.url_for("static", filename="*.html"))

cors = CORS(app, resources={r"/uploader": {"origins": "*"}})

cors = CORS(app, resources={r"/columns": {"origins": "*"}})
cors = CORS(app, resources={r"/delete": {"origins": "*"}})
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER




 


@app.route('/')
def hello():
    return render_template('index.html')
    
@app.route('/uploader', methods = ['GET', 'POST'])


def upload_file():
   #str ="C:/Users/Fares/Desktop/pfe/"
   if request.method == 'POST':
      f = request.files['file']
      filename = secure_filename(f.filename)
      f.save(os.path.join(app.config['UPLOAD_FOLDER']+ filename))
      filePath = os.path.join(app.config['UPLOAD_FOLDER'] + filename)
      naneview = profiling.dataset_report(filename)
      print("filename :" + filePath)
      item = {}
      item["path"] = filePath
      mycollection.insert_one(item)

      #naneview = profiling.json_report(filename)

      return naneview
      
@app.route('/Duplicate', methods = ['GET', 'POST'])

def Duplicate():
   profiling.deleteDuplicate()
   return 'hi'

@app.route('/pathListe' , methods =['GET'])
def paths_list():
    liste=[]
    paths = mycollection.find({},{"_id":0})
    for i in paths:
        liste.append(i)
    return jsonify(liste)


# tekhou l path mtaa l fichier w traja3li les colonnes mte3ou 

@app.route('/columns' , methods =['POST'],)
 
def columnListFromPath():
      path = request.values.get("path", type=str, default=None)
      
      if (path.split(".")[1]== "csv") :
            df = pd.read_csv(path)
            dataColumn = df.columns
      elif(path.split(".")[1] == "xlsx"):
            df = pd.read_excel(path)
            dataColumn = df.columns
      lst=[]
      for x in dataColumn:
         lst.append(x)
      print(lst)
     
      return jsonify(lst)

#tekhou l path mtaa l fichier w les colonnes eli theb taamel aalehom drop_duplicates w trajaalek fichier manghir doublons 
@app.route('/delete' , methods =['POST'])
 
def deletecolumn():
      #path = request.values.get("path", type=str, default=None)
      #columnsList = request.values.get("columnsList", type=json, default=None)
      content = request.get_json()
      date = datetime.now()
      print(date)
      dateToString = date.strftime("%A")
      date2 = dateToString +"-"+date.strftime("%d-%B-%Y-%H-%M-%S-") 
      #y  = dateToString.split(" ")
      #z = y[0] +"-"+ y[1].split(".")[0]
      print(content['path'])
      print(content['columnsList'])
      if (content['path'].split(".")[1]== "csv") :
            df = pd.read_csv(content['path'])
            
      elif(content['path'].split(".")[1] == "xlsx"):
            df = pd.read_excel(content['path'])
      datasplit =  content['path'].split("/")[6]

      for elem in content["columnsList"] :
         print(elem)
         
         df.drop_duplicates(subset=[str(elem)],keep="first",inplace=True)
      nomfile=date2+datasplit 
      df.to_csv(r'new_datasets_without_duplicate/new_'+nomfile   ,index=True)
      print(dateToString)

      newPath = 'new_datasets_without_duplicate/new_' +nomfile
      item = {}
      item["path"] = newPath
      mycollection2.insert_one(item)
     
      return jsonify('new_datasets_without_duplicate/new_'+nomfile)

@app.route('/autocleaning' , methods =['POST'],)
def tt():
    
        
    df= pd.read_csv('new_datasets/*.csv')

    fpath=glob.glob("new_datasets/*.csv")
    print(fpath)
    datasplit = fpath.split('/')[1]
    pipeline = AutoClean(df)
    ncsv = pipeline.output
    ncsv.to_csv('new_datasets/new_autoclean' + datasplit)

    return jsonify('new_datasets/new_autoclean' + datasplit)

@app.route('/autoclean', methods=['POST'])
def autoclean():
      content = request.get_json()
      contentPath = content['path']
      result = profiling.autoclean(contentPath)

      return result 

@app.route('/autocleanwithparams', methods=['POST'])
def autocleanwithParams():
      content = request.get_json()
      path = content['path']


      if content['missingNum']:
            missingNum = content['missingNum']

      elif content['missingCat']:
            missingCat = content['missingCat']
    

      elif content['outlierParam']:
            outlierParam = content['outlierParam']
 
      elif content['extractDateTime']:
            extractDateTime = content['extractDateTime']
      elif content['outliers']:
            outliers = content['outliers']
      else :
            extractDateTime="False"
            outlierParam=1.5
            missingCat ="False"
            missingNum="False"
            outliers="winz"


      result = profiling.autocleanwithParams(path=path , missingNum=missingNum , missingCat=missingCat ,outliers= outliers,outlierParam= outlierParam ,extractDateTime= extractDateTime)

      return result 
@app.route('/hash' , methods=['POST'])
def hashData() :
 content = request.get_json()
 path = content['path']
 list = content['list']
 profiling.hashColumn(path = path, listColumn =list )

 return 'hash'






if __name__ == '__main__':
   app.run(debug = True)