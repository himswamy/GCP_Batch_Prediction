"Created by Himanshu swamy"

import datetime
import pandas as pd
import numpy as np
import pymysql
import pymysql.cursors
from os import getenv
import sqlalchemy
from google.cloud import storage
from sklearn.externals import joblib
from google.cloud import storage
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
from sklearn.feature_selection import SelectKBest
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelBinarizer
import googleapiclient.discovery
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import Imputer
from sklearn.preprocessing import MinMaxScaler, Imputer

BUCKET_NAME = 'raw_data_scripts'

#loaded_model = joblib.load('model.joblib')


# TODO(developer): specify SQL connection details
CONNECTION_NAME = getenv(
  'INSTANCE_CONNECTION_NAME',
  'credit-default:us-central1:credit-work')
DB_USER = getenv('MYSQL_USER', 'root')
DB_PASSWORD = getenv('MYSQL_PASSWORD', 'root')
DB_NAME = getenv('MYSQL_DATABASE', 'credit_risk')
 
mysql_config = {
  'host': '104.154.35.150',
  'user': DB_USER,
  'password': DB_PASSWORD,
  'db': DB_NAME,
  'charset': 'utf8mb4',
  'cursorclass': pymysql.cursors.DictCursor,
  'autocommit': True
}

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(DB_USER, DB_PASSWORD, 
                                                      '104.154.35.150', DB_NAME))
 

connection = pymysql.connect(**mysql_config)
connection1 = database_connection.connect()

try:
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT * FROM new_data"
        cursor.execute(sql)
        sql_data = pd.DataFrame(cursor.fetchall())
        app_test = sql_data

        #Data cleaning 

        #Label Encoding

        le = LabelEncoder()
        le_count = 0
        for col in app_test:
            if app_test[col].dtype == 'object':
                # If 2 or fewer unique categories
                if len(list(app_test[col].unique())) <= 2:
                    # Train on the train_test data
                    le.fit(app_test[col])
                    # Transform train_test data
                    app_test[col] = le.transform(app_test[col])
        
        app_test = app_test.drop(columns = ['SK_ID_CURR'])
        test = app_test.copy()

        test = test.fillna(0)
        test = test.drop(columns = ['CODE_GENDER'])

        #log_reg_pred = loaded_model.predict_proba(test)[:, 1]

        PROJECT_ID = 'credit-default'
        VERSION_NAME = 'version_1'
        MODEL_NAME = 'version1'

        service = googleapiclient.discovery.build('ml', 'v1')
        name = 'projects/{}/models/{}'.format(PROJECT_ID, MODEL_NAME)
        name += '/versions/{}'.format(VERSION_NAME)
        complete_results = []
        for data in [test]:
            responses = service.projects().predict(
                name=name,
                body={'instances': data}
            ).execute()

            if 'error' in responses:
                print(response['error'])
            else:
                complete_results.extend(responses['predictions'])
        predictions = pd.DataFrame({'predicts_probability':complete_results})

        dataset_predict = pd.concat([sql_data, predictions], axis=1)

        dataset_predict.to_sql(con=connection1, name='predict_data', if_exists='replace', index=False)

        #print(dataset_predict.head(10))

finally:
    connection.close()
