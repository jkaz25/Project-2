# Assignment: project2.py
# Programmer: Joey Kaz
# Date: March 31, 2024
# This Python script is used to emulate QA testing by performing SQL queries on data entries through the use of MySQL server,
# and uses the Pandas module for data manipulation
#The program uses argparse for user input and performs the following primary operations with data:
# 1) importing csv files into the database tables
# 2) finding all entries on a specific date
# 3) finding all bugs considered to be repeatable and blockers
# 4) Drop duplicate entries based upon 'TestCase' , 'ExpectedResult' , and 'ActualResult' entries

#import modules for program
import mysql.connector
import pandas as pd
import argparse as ap
import os

#create argparse argument parser and add arguments
parser = ap.ArgumentParser(description='Input file to create dataframe')
parser.add_argument("file",choices=['collection1', 'collection2'], help='The name of the file to import or query.')
parser.add_argument('-d', '--dump', nargs='+', type=str, dest='dump', help='Add one or more csv files')
parser.add_argument('-e', '--ends', action='store_true', help='Retrieve the first, middle, and last entry in a collection')
parser.add_argument('-u', '--user', nargs=2, type=str, dest='user', help='Retrieve entries for specified user from both collections without duplicates')
parser.add_argument('-b', '--blocker',action='store_true', help='Find all blocker bugs from both collections without duplicates')
parser.add_argument('-r', '--repeatable', action='store_true', help='Find all repeatable bugs from both collections without duplicates')
parser.add_argument('-da', '--date', nargs=1, type=str, dest='date', help='Search for bugs from a particular date without duplicates. Enter in m/d/yyyy')
args = parser.parse_args()

#create connection with mysql server local instance
def getConnection():
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Joesam11",
        database = "project2"
    )
    return mydb

#method that fixes strings from csv cells by stripping whitespace, replacing new line characters with a space, and replacing double spaces with a single space
def replace(attribute):
    attribute = str(attribute).replace("\n", " ")
    split = attribute.split("  ")
    newAttribute = ""
    for part in split:
        part = part.lstrip()
        part = part.rstrip()
        newAttribute = newAttribute + " " + part
    return newAttribute

#function that will create the specified collection for insertion and drop the table if it already exists
def createCollection(cursor, collection:str):
    sql = f"DROP TABLE IF EXISTS {collection}"
    cursor.execute(sql)
    sql = f"CREATE TABLE {collection} (EntryID int AUTO_INCREMENT, TestNumber varchar(1000), Date varchar(1000), Category varchar(1000), TestCase varchar(1000), ExpectedResult varchar(1000), ActualResult varchar(1000), Repeatable varchar(1000), Blocker varchar(1000), Owner varchar(1000), PRIMARY KEY (EntryID))"
    cursor.execute(sql)
    connection.commit()

#method that inserts records from all specified csv files into the specified collection
def insert(records:list, collection: str):
    sql = f'INSERT INTO {collection} (TestNumber, Date, Category, TestCase, ExpectedResult, ActualResult, Repeatable, Blocker, Owner) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for row in records:
        test_number = replace(str(row[1]))
        date = replace(str(row[2]))
        category = replace(str(row[3]))
        test_case = replace(row[4])
        expected = replace(row[5])
        actual = replace(row[6])
        repeatability = replace(row[7])
        repeatability = repeatability.lower()
        blocker = replace(row[8])
        blocker = blocker.lower()
        test_owner = replace(row[9])
        val = (test_number[1:], date[1:], category[1:], test_case[1:], expected[1:], actual[1:], repeatability[1:], blocker[1:], test_owner[1:])
        cursor.execute(sql, val)
    connection.commit()

def printResults(resultsFrame,testName:str):
    if os.path.exists(f"{testName}.xlsx"):
        os.remove(f"{testName}.xlsx")
    header = ['EntryID','TestNumber','Date','Category','TestCase','ExpectedResult','ActualResult','Repeatable','Blocker','Owner']
    resultsFrame.to_excel(f"{testName}.xlsx",columns=header)
    print(f'Number of records {len(resultsFrame)}')

#function that reads all records from csv and returns a dataframe 
def readRecords(fileName):
    df = pd.read_csv(fileName)
    insertList = [record for record in df.itertuples()]
    return insertList

#function that reads entries from Professor Chaja and outputs the results as a spreasheet
def chaja(chajaQuery:str):
    if os.path.exists("chajaOutput.xlsx"):
        os.remove("chajaOutput.xlsx")
    df = pd.read_sql(chajaQuery, connection)
    header = ['EntryID','TestNumber','Date','Category','TestCase','ExpectedResult','ActualResult','Repeatable','Blocker','Owner']
    df.to_excel("chajaOutput.xlsx",columns=header)

#function that queries both collections, removes duplicates based off of the Actual Result category, then returns the dataframe
def query_concat_and_drop(sql1:str, sql2:str):
    df = pd.read_sql(sql1,connection) # read from collection1 (user-specific entries)
    df2 = pd.read_sql(sql2, connection) #read from collection2 (all user entries)
    df2 = pd.concat([df2,df]) # merge both dataframes together into one large collection of records
    df2 = df2.drop_duplicates(subset=['TestCase','ExpectedResult','ActualResult'],keep='last') # drop duplicate records that share the same 'TestCase', 'ExpectedResult', and 'ActualResult' attribute values
    df2 = df2.dropna()
    return df2 

#function that queries both collections for entries by a specified user
def user(user: str):
    sql1 = f"SELECT * FROM collection1 WHERE Owner = '{user}'"
    sql2 = f"SELECT * FROM collection2 WHERE Owner = '{user}'"
    #check if user is Kevin Chaja, else call concat and drop method
    df = pd.DataFrame(query_concat_and_drop(sql1, sql2))
    if user == "Kevin Chaja":
        printResults(df,"chajaOutput")
        return
    printResults(df,"users")

#function that reports the first, middle, and last entries for QA testing
def ends():
    sql1 = f"SELECT * FROM collection2"
    df = pd.read_sql(sql1, connection)
    length = len(df)
    ends = [tuple(df.iloc[0]), tuple(df.iloc[length//2]), tuple(df.iloc[length-1])]
    print(f'First Result {ends[0]}')
    print(f'Middle Result {ends[1]}')
    print(f'Last Result {ends[2]}')

#function that queries for all reportable bugs that were repeatable
def repeats():
    sql1 = f"select * from collection1 where Repeatable Like 'y%'"
    sql2 = f"select * from collection2 where Repeatable Like 'y%'"
    df = pd.DataFrame(query_concat_and_drop(sql1, sql2))
    printResults(df,"repeats")

#function that queries for all reportable bugs that were considered to be blockers from the user
def blockers():
    sql1 = f"select * from collection1 where Blocker Like 'y%'"
    sql2 = f"select * from collection2 where Blocker Like 'y%'"
    df2 =query_concat_and_drop(sql1,sql2)
    printResults(df2,'blockers')
    
#query that returns all entries for bugs on a specified date from the user
def getDateEntries(date:str):
    sql1 = f"select * from collection1 where Date Like '%{date}%'"
    sql2 = f"select * from collection2 where Date Like '%{date}%'"
    df2 =query_concat_and_drop(sql1, sql2)
    printResults(df2,"dates")
    
#connection object with MySQL server
connection = getConnection()
#cursor object for retrieving query result sets
cursor = connection.cursor()

selectedCollection = None    

#check which collection the user wishes to use
match args.file:
    case 'collection1':
        selectedCollection = 'collection1'
    case 'collection2':
        selectedCollection = 'collection2'
    case _:
        #exit program if incorrect collection is selected
        print(f"No such collection: '{args.file}' exists")
        exit(0)

#check which arguments from the argument parser were chosen by the user and execute them
if args.dump:
    createCollection(cursor, args.file)
    for file in args.dump:
        insertList = readRecords(file)
        insert(insertList, selectedCollection)

if args.ends:
    ends()

if args.user:
    userString = args.user[0] + " " + args.user[1]
    user(userString)

if args.repeatable:
    repeats()

if args.blocker:
    blockers()

if args.date:
    getDateEntries(args.date[0])