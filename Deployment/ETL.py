import pandas as pd
import pandasql as ps
import os

##Please set here your working paths before to execute the ETL process

##Here we found the input data
myInputPath = "C:\\Users\\cemora\Documents\\GitHub\\de-challenge\\data\\"

##Here we are going to store the Data Model
myDataModelPath = "C:\\Users\\cemora\Documents\\GitHub\\de-challenge\\data\\DataModel\\"

##Here we are going to push the output Reports
myOutputPath = "C:\\Users\\cemora\Documents\\GitHub\\de-challenge\\data\\OutputReports\\"

print("Getting the input data from : " + myInputPath)
#Reading consoles input data 
try:
    dfConsoles = pd.read_csv(myInputPath + 'consoles.csv')
    nrecords = str(dfConsoles.shape[0])
    print("- consoles dataset loaded sucessfully : " + nrecords + " records")
except:
    print("- consoles input dataset not found, ending the process")
    exit()

try:
    dfResult = pd.read_csv(myInputPath + 'result.csv')
    nrecords = str(dfResult.shape[0])
    print("- result dataset loaded sucessfully : " + nrecords + " records")
except:
    print("- result input dataset not found, ending the process")
    exit()

#Data Normalization
#Company LookUp table creation
q1 = """SELECT DISTINCT(company) as CompanyName FROM dfConsoles """
dfCompanyTable = ps.sqldf(q1, locals())
dfCompanyTable.insert(0, 'CompanyId', dfCompanyTable.index + 1)
dfCompanyTable.to_csv(myDataModelPath + 'companyTable.csv', index = False)

#Console Lookup table creation
q1 = """SELECT DISTINCT(console) as ConsoleName, company as CompanyName FROM dfConsoles """
dfConsoleTable = ps.sqldf(q1, locals())
dfConsoleTable.insert(0, 'ConsoleId', dfConsoleTable.index + 1)
dfConsoleTable=pd.merge(dfConsoleTable,dfCompanyTable,how='inner',on='CompanyName')
dfConsoleTable = dfConsoleTable[['ConsoleId', 'ConsoleName','CompanyId']]
dfConsoleTable.to_csv(myDataModelPath + 'consoleTable.csv', index = False)

#Games LookUp table creation
q1 = """SELECT DISTINCT(name) as GameName FROM dfResult """
dfGameTable = ps.sqldf(q1, locals())
dfGameTable.insert(0, 'GameId', dfGameTable.index + 1)
dfGameTable.to_csv(myDataModelPath + 'gameTable.csv', index = False)


#Result fact table creation
dfResultTable=pd.merge(dfResult,dfConsoleTable,how='inner',left_on='console', right_on='ConsoleName')
dfResultTable = dfResultTable[['metascore', 'name','ConsoleId','userscore','date']]
dfResultTable=pd.merge(dfResultTable,dfGameTable,how='inner',left_on='name', right_on='GameName')
dfResultTable = dfResultTable[['metascore', 'GameId','ConsoleId','userscore','date']]
dfResultTable.to_csv(myDataModelPath + 'ResultTable.csv', index = False)

print("Data Cleaning")
#Cleaning data
dfResultTable = dfResultTable.drop(dfResultTable[dfResultTable.userscore == "tbd"].index)
cleannrecords = str(dfResultTable.shape[0])
cleanednrecords = str(int(nrecords) - int(cleannrecords))
print("- Number of records removed due invalid userscore : " + cleanednrecords)
print("- Number of records in result dataset after cleaning : " + cleannrecords)

print("Report Generation")
#Creating ETL Output 1 : Top 10 best games by Company/Console
df1 = dfResultTable.sort_values(['ConsoleId','userscore'], ascending = False).groupby('ConsoleId').head(10)
df2 = df1[['ConsoleId', 'GameId','userscore']]
df3=pd.merge(dfConsoleTable,df2,how='inner',on='ConsoleId')
df3 = df3[['CompanyId','ConsoleName', 'GameId','userscore']]
df3=pd.merge(dfGameTable,df3,how='inner',on='GameId')
df3 = df3[['CompanyId', 'ConsoleName', 'GameName', 'userscore']]
df3=pd.merge(dfCompanyTable,df3,how='inner',on='CompanyId')
df3 = df3[['CompanyName', 'ConsoleName', 'GameName', 'userscore']]
q1 = """SELECT CompanyName, ConsoleName, GameName, userscore FROM df3 order by 1 asc,2 asc, 4 desc"""
df3 = ps.sqldf(q1, locals())
df3.to_csv(myOutputPath + 'Top10byConsole.csv', index = False)
print("1.- Top 10 best games for each console/company Report: Done")
###################################################################################################

#Creating ETL Output 2 : Top 10 worst games by Company/Console
df1 = dfResultTable.sort_values(['ConsoleId','userscore'], ascending = True).groupby('ConsoleId').head(10)
df2 = df1[['ConsoleId', 'GameId','userscore']]
df3=pd.merge(dfConsoleTable,df2,how='inner',on='ConsoleId')
df3 = df3[['CompanyId','ConsoleName', 'GameId','userscore']]
df3=pd.merge(dfGameTable,df3,how='inner',on='GameId')
df3 = df3[['CompanyId', 'ConsoleName', 'GameName', 'userscore']]
df3=pd.merge(dfCompanyTable,df3,how='inner',on='CompanyId')
df3 = df3[['CompanyName', 'ConsoleName', 'GameName', 'userscore']]
q1 = """SELECT CompanyName, ConsoleName, GameName, userscore FROM df3 order by 1 asc,2 asc, 4 asc"""
df3 = ps.sqldf(q1, locals())
df3.to_csv(myOutputPath + 'Worst10byConsole.csv', index = False)
print("2.- Worst 10 games for each console/company Report: Done")
###################################################################################################

#Creating ETL Output 3 : Top 10 Games for all consoles
df4 = dfResultTable.sort_values('userscore', ascending = False).head(10)
df4 = df4[['ConsoleId', 'GameId','userscore']]
df4=pd.merge(dfConsoleTable,df4,how='inner',on='ConsoleId')
df4 = df4[['ConsoleName', 'GameId','userscore']]
df4=pd.merge(dfGameTable,df4,how='inner',on='GameId')
df4 = df4[['ConsoleName', 'GameName','userscore']]
q1 = """SELECT ConsoleName, GameName, userscore FROM df4 order by 3 desc"""
df4 = ps.sqldf(q1, locals())
df4.to_csv(myOutputPath + 'Top10AllConsoles.csv', index = False)
print("3.- Top 10 games for all consoles Report: Done")
###################################################################################################

#Creating ETL Output 4 : Worst 10 Games for all consoles
df4 = dfResultTable.sort_values('userscore', ascending = True).head(10)
df4 = df4[['ConsoleId', 'GameId','userscore']]
df4=pd.merge(dfConsoleTable,df4,how='inner',on='ConsoleId')
df4 = df4[['ConsoleName', 'GameId','userscore']]
df4=pd.merge(dfGameTable,df4,how='inner',on='GameId')
df4 = df4[['ConsoleName', 'GameName','userscore']]
q1 = """SELECT ConsoleName, GameName, userscore FROM df4 order by 3 asc"""
df4 = ps.sqldf(q1, locals())
df4.to_csv(myOutputPath + 'Worst10AllConsoles.csv', index = False)
print("4.- Worst 10 games for all consoles Report: Done")
###################################################################################################
print("Process Ended, check the output Reports on : " + myOutputPath)
