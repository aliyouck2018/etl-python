"""
ETL WITH PYTHON

A simple script to demonstrate the ETL (Extract Transform Load) process with python by pulling data from a csv file,
performing some data cleaning and finally save it to a database on MySQL.The dataset is from kaggle and contains sample 
data from a retail company. It is available at this address: https://www.kaggle.com/datasets/sahilprajapati143/retail-analysis-large-dataset

Some of the data cleaning have been done right in the csv file because they would have been more challenging to do in the dataframe.
It involves filling the null values in the Total_Purchases, Amount and Total_Amount fields with calculated ones.
The process looks like this: Filter the data to show only rows with null in Total_Purchases, select the first cell of this column and 
enter the formula '= Total_Amount/Amount' after making Total_Purchases an integer, drag the formula down in the following cells. 
Repeat the same process for the two others, with the formulas '=Total_Amount/Total_Purchases' and '=Total_Purchases*Amount'.

After the that we just rename/remove some columns and fill the remaning null values with unknown.

"""

import pandas as pd
import mysql.connector
from datetime import datetime

print('Loading the csv file...')
transactions = pd.read_csv('retail_data.csv',sep=',')

""" 
After a close look a the transactions, we can see that we dont necessarely 
need the State, Year, Month and Time columns so we can delete them.

"""

print('Deleting unwanted columns...')
del transactions['State']
del transactions['Year']
del transactions['Month']
del transactions['Time']


# After that we need to rename the some columns 
print('Renaming columns...')
transactions.rename(columns={'Transaction_ID'  :'TransactionID'},inplace=True)
transactions.rename(columns={'Customer_ID'     :'CustomerID'},inplace=True)
transactions.rename(columns={'Zipcode'         :'Zip Code'},inplace=True)
transactions.rename(columns={'Income'          :'Income Level'},inplace=True)
transactions.rename(columns={'Customer_Segment':'Customer Segment'},inplace=True) 
transactions.rename(columns={'Date'            :'Date of Purchase'},inplace=True)
transactions.rename(columns={'Total_Purchases' :'Items Purchased'},inplace=True)
transactions.rename(columns={'Amount'          :'Price per Item'},inplace=True)
transactions.rename(columns={'Total_Amount'    :'Total Amount'},inplace=True)
transactions.rename(columns={'Product_Category':'Product Category'},inplace=True)
transactions.rename(columns={'Product_Brand'   :'Product Brand'},inplace=True)
transactions.rename(columns={'Product_Type'    :'Product Type'},inplace=True)
transactions.rename(columns={'Shipping_Method' :'Shipping Method'},inplace=True)
transactions.rename(columns={'Payment_Method'  :'Payment Method'},inplace=True)
transactions.rename(columns={'Order_Status'    :'Order Status'},inplace=True)
transactions.rename(columns={'Ratings'         :'Rating'},inplace=True)
transactions.rename(columns={'products'        :'Product'},inplace=True)


# Replace null values with unknown
print('Replacing null values...')
transactions.fillna('Unknown',inplace=True)

"""
After cleaning the dataset we can now save it to the database using the mysql-connector module.
We start by establishing the connection by providing the credentials, then we iterate through the dataframe
and insert each row one by one before close the connection.
For the sake of simplicity we made the 'Date of Purchase' field a varchar.

"""

DB_NAME = 'retail'
TABLES = {}
TABLES['transactions'] = (
    "CREATE TABLE `transactions` ("
    "  `TransactionID` varchar(40),"
    "  `CustomerID` varchar(40),"
    "  `Name` varchar(40) ,"
    "  `Email` varchar(40) ,"
    "  `Phone` varchar(40) ,"
    "  `Address` varchar(40) ,"
    "  `City` varchar(40) ,"
    "  `ZipCode` varchar(40) ,"
    "  `Country` varchar(40) ,"
    "  `Age` int ,"
    "  `Gender` varchar(40) ,"
    "  `IncomeLevel` varchar(40) ,"
    "  `CustomerSegment` varchar(40) ,"
    "  `DateOfPurchase` varchar(10) ,"
    "  `ItemsPurchased` int ,"
    "  `PricePerItem` float ,"
    "  `TotalAmount` float ,"
    "  `ProductCategory` varchar(40) ,"
    "  `ProductBrand` varchar(40) ,"
    "  `ProductType` varchar(40) ,"
    "  `Feedback` varchar(40) ,"
    "  `ShippingMethod` varchar(40) ,"
    "  `PaymentMethod` varchar(40) ,"
    "  `OrderStatus` varchar(40) ,"
    "  `Rating` varchar(40) ,"
    "  `Product` varchar(40), "
     "  PRIMARY KEY (`TransactionID`)"
    ") ENGINE=InnoDB")

# Initialize connection to MySQL Server
connection = mysql.connector.connect(user='username',password='password')
cursor = connection.cursor()

# Helper functions 
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)


def create_table(cursor):
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


def insert_data(cursor):
	print('Inserting data ...')
	for index, transaction in transactions.iterrows():
	    add_transaction = ("INSERT INTO transactions "
	       "(TransactionID,CustomerID,Name,Email,Phone,Address,City,ZipCode,Country,Age,Gender,IncomeLevel,CustomerSegment,DateOfPurchase,ItemsPurchased,PricePerItem,TotalAmount,ProductCategory,ProductBrand,ProductType,Feedback,ShippingMethod,PaymentMethod,OrderStatus,Rating,Product) "
	       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

	    transaction_ = (
	        transaction['TransactionID'],
	        transaction['CustomerID'],
	        transaction['Name'],
	        transaction['Email'],
	        transaction['Phone'],
	        transaction['Address'],
	        transaction['City'],
	        transaction['Zip Code'],
	        transaction['Country'],
	        transaction['Age'],
	        transaction['Gender'],
	        transaction['Income Level'],
	        transaction['Customer Segment'],
	        transaction['Date of Purchase'],
	        transaction['Items Purchased'],
	        transaction['Price per Item'],
	        transaction['Total Amount'],
	        transaction['Product Category'],
	        transaction['Product Brand'],
	        transaction['Product Type'],
	        transaction['Feedback'],
	        transaction['Shipping Method'],
	        transaction['Payment Method'],
	        transaction['Order Status'],
	        transaction['Rating'],
	        transaction['Product'])

	    cursor.execute(add_transaction, transaction_)  

create_database()
create_table(cursor)
insert_data(cursor)

connection.commit()
cursor.close()
connection.close()