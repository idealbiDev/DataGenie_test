from flask import Flask, jsonify
import pymysql
import os
import pyodbc


DB_SERVER = os.getenv("DB_SERVER")
print(DB_SERVER)
def get_conn():
    try:
        # Attempt to establish a connection
        conn = pyodbc.connect(DB_SERVER)
        return conn
    except pyodbc.Error as e:
        # Raise the error to be caught in the route
        raise e
    
def get_db_connection():
    try:
        dg_conn = pymysql.connect(
            host="102.222.124.10",
            user="idealbk8c9x0_app_usr",
            password="idealbi_app_user",
            database="idealbk8c9x0_ibisols",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor  # Returns rows as dictionaries
        )
        return dg_conn
    except pymysql.Error as err:
        print(f"Database connection error: {err}")
        return None