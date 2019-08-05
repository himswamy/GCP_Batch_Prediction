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


BUCKET_NAME = 'raw_data_scripts'


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
        sql = "SELECT * FROM train_test_data"
        cursor.execute(sql)
        sql_data = pd.DataFrame(cursor.fetchall())
        app_train = sql_data

        #Data cleaning 

        #Label Encoding and One-Hot Encoding

        le = LabelEncoder()
        le_count = 0
        for col in app_train:
            if app_train[col].dtype == 'object':
                # If 2 or fewer unique categories
                if len(list(app_train[col].unique())) <= 2:
                    # Train on the train_test data
                    le.fit(app_train[col])
                    # Transform train_test data
                    app_train[col] = le.transform(app_train[col])
        

        app_train = app_train.drop(columns = ['SK_ID_CURR'])
        train_labels = app_train['TARGET_var']
        train = app_train.drop(columns = ['TARGET_var'])

        train = train.fillna(0)
        train = train.drop(columns = ['CODE_GENDER'])

        classifier = LogisticRegression(C = 0.0001)
        classifier.fit(train, train_labels)
        

        pipeline = Pipeline([
            ('classifier', classifier)
        ])

        model = 'model.joblib'
        joblib.dump(pipeline, model)

        # Upload the model to GCS
        bucket = storage.Client().bucket(BUCKET_NAME)
        blob = bucket.blob('{}/{}'.format(
            datetime.datetime.now().strftime('credit_%Y%m%d_%H%M%S'),
            model))
        blob.upload_from_filename(model)

        print(model)

finally:
    connection.close()
