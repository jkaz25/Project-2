# QA Replacement Script
This project is meant to emulate data management for QA in media entertainment. The project uses multiple Python frameworks including Pandas, argparse, and mysql connector with a MySQL local instance.
This Python script is used to emulate QA testing by performing SQL queries on data entries through the use of MySQL server,
and uses the Pandas module for data manipulation
The program uses argparse for user input and performs the following primary operations with data:
1) Importing CSV files into the database tables
2) finding all entries on a specific date
3) Finding all bugs considered to be repeatable and blockers
4) Drop duplicate entries based upon 'TestCase', 'ExpectedResult', and 'ActualResult' entries

There are 2 implementations for this project, one using mysql.connector/MySQL and a second implementation using pymongo/MongoDB. Both implementations yield identical results and are listed under the 
code files.
