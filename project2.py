# Assignment: project2.py
# Programmer: Joey Kaz
# Date: March 31, 2024
# This Python script is used to emulate QA testing by performing SQL queries on data entries through the use of MongoDB local instance,
# and uses the Pandas module for data manipulation
#The program uses argparse for user input and performs the following primary operations with data:
# 1) importing csv files into the database collections
# 2) finding all entries on a specific date
# 3) finding all bugs considered to be repeatable and blockers through regular expression

import argparse as ap
import pymongo
import pandas as pd
import os

#establish connection with MongoDB local host 
myclient = pymongo.MongoClient('mongodb://localhost:27017/')

#create database and 2 collections
mydb = myclient["project"]
mycollection1 = mydb['collection1']
mycollection2 = mydb['collection2']

#create argparse argument parser and add arguments
parser = ap.ArgumentParser(description='Input file to create dataframe')
parser.add_argument("file",choices=['collection1', 'collection2'], help='The name of the file to import or query.')
parser.add_argument('-d', '--dump', nargs='+', type=str, dest='dump', help='Add one or more csv files')
parser.add_argument('-e', '--ends', action='store_true', help='Retrieve the first, middle, and last entry in a collection')
parser.add_argument('-u', '--user', nargs=2, type=str, dest='user', help='Retrieve entries for specified user')
parser.add_argument('-b', '--blocker',action='store_true', help='Find all blocker bugs from tests')
parser.add_argument('-r', '--repeatable', action='store_true', help='Find all repeatable bugs')
parser.add_argument('-da', '--date', nargs=1, dest='date', help='Search for bugs from a particular date. Enter in m/d/yyyy')
parser.add_argument('-du', '--duplicates', action='store_true', help='Find all duplicates within the mega dump.')
args = parser.parse_args()

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

#parse CSV inputs to crate database insert records
def insertRecords(df, collection):
    count = 0
    for row in df.itertuples():
        count = count + 1
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

        #creat and insert record
        x = collection.insert_one(
            {"Test Number" : test_number[1:], 
            "Date" : date[1:], 
            "Category": category[1:],
            "Test Case" : test_case[1:],
            "Expected Result": expected[1:],
            "Actual Result": actual[1:],
            "Repeatable?": repeatability[1:],
            "Blocker" : blocker[1:],
            "Owner": test_owner[1:]
            }
        )
    return count

#displays formatted results
def printResults(resultsFrame, testName:str):
    #delete previous output and replace with newly run output
    if os.path.exists(f"{testName}.xlsx"):
        os.remove(f"{testName}.xlsx")
    header = ['Test Number','Date','Category','Test Case','Expected Result','Actual Result','Repeatable?','Blocker','Owner']
    resultsFrame.to_excel(f"{testName}.xlsx",columns=header)
    print(f'Number of records {len(resultsFrame)}')
    print(f"Please see spreadsheet {testName}.xlsx for result set")

#parses list of CSV arguments for insertion
def dumpFiles(fileList, collection):
        count = 0
        for file in fileList:
            df = pd.read_csv(str(file))
            count = count + insertRecords(df, collection)
        print(f"Successfully inserted {count} records into {collection.name}")

#function that queries both collections, removes duplicates based off of the Actual Result category, then returns the dataframe
def query_and_dropDups(query):
    df = pd.DataFrame(mycollection1.find(query, {"_id" : 0}))
    df2 = pd.DataFrame(mycollection2.find(query, {"_id" : 0}))
    df2 = pd.concat([df2,df])
    #drops duplicate entries sharing the same test case, expected result, and actual result fields
    newdf = df2.drop_duplicates(subset=['Test Case','Expected Result','Actual Result'],keep='last')
    return newdf

#method that finds all entries for a specified user
def findUserEntries(user):
    query = {'Owner' : {'$regex' : f'^{user}'}}
    df = query_and_dropDups(query)
    #check for user Kevin Chaja
    if user == "Kevin Chaja":
        printResults(df,"chajaOutput")
        return
    printResults(df,"users")

#method that finds all entries marked as blockers by the user
def findBlockers():
    query = {'Blocker' : {'$regex': '^y'}}
    df = query_and_dropDups(query)
    printResults(df,"blockers")

#method that finds all entries marked as repeatable by the user
def findRepeats():
    query = {'Repeatable?': {'$regex': "^y"}}
    df = query_and_dropDups(query)
    printResults(df,"repeats")

#method that finds all entries on a specific date 
def findDates(date):
    query = {'Date' : {'$regex' : f'{date}'}}
    df = query_and_dropDups(query)
    printResults(df,"dates")

def printRow(row):
    print(f"Test Number: {row[1]} | Date: {row[2]} | Category: {row[3]} \nTest Case: {row[4]} | Expected Result: {row[5]} | \nActual Result: {row[6]} | Repeatable?: {row[7]}"
         + f" Blocker: {row[8]} | Owner: {row[9]}\n")

#method that finds the first, middle, and final entries in a collection
def ends(collection):
    df = pd.DataFrame(collection.find())
    length = len(df)
    ends = pd.DataFrame([tuple(df.iloc[0]), tuple(df.iloc[length//2]), tuple(df.iloc[length-1])])
    printRow(tuple(df.iloc[0]))
    printRow(tuple(df.iloc[length//2]))
    printRow(tuple(df.iloc[length-1]))

# creating memory reference to eventual selected collection from program arguments
selectedCollection = None    

#check which collection the user wishes to use
match args.file:
    case 'collection1':
        selectedCollection = mycollection1
    case 'collection2':
        selectedCollection = mycollection2
    case _:
        #exit program if incorrect collection is selected
        print(f"No such collection: '{args.file}' exists")
        exit(0)

#check which arguments from the argument parser were chosen by the user and execute them
if args.dump:
    dumpFiles(args.dump, selectedCollection)

if args.ends:
    ends(selectedCollection)

if args.user:
    name = args.user[0] + " " + args.user[1]
    findUserEntries(str(name))

if args.blocker:
    findBlockers()

if args.repeatable:
    findRepeats()

if args.date:
    findDates(str(args.date[0]))