def CreateDatabase(DataUser):

    """
        This subroutine creates the database that will be used by Health Data.
        It drops the existing database and all the data tables. If it is run,
        all the data that is currently included in the tables will be lost.
        The option to access this routine from the menu is restricted to users
        that have Create_priv on the server.
    """
    import mariadb

    # Open cursor for data queries using the Connection in DataUser
    DataUser.NewConnect()
    cur = DataUser.Connection.cursor()

    # Map of relational database A
    HDCompString =  '\t+{:-<22}+\n'.format('')
    HDCompString += '\t| {:^20} |\n'.format('healthdata')
    HDCompString += '\t| {:^20} |\n'.format('Database')
    HDCompString += '\t+{:-<22}+\n'.format('')
    print(HDCompString)

    # Create database heathdata
    DatabaseIntStringA = 'DROP DATABASE IF EXISTS healthdata;'
    DatabaseIntStringB = 'CREATE DATABASE IF NOT EXISTS healthdata;'
    DatabaseIntStringC = 'USE healthdata;'
    try:
        cur.execute(DatabaseIntStringA)
        cur.execute(DatabaseIntStringB)
        cur.execute(DatabaseIntStringC)
    # If an error occurs executing the queries, then print the error and location.
    except mariadb.Error as e1:
        print('Error creating Database healthdata: {error}'.format(error=e1))

    # Create table brfssdata
    DataBRFStringA = 'DROP TABLE IF EXISTS brfssdata;'
    DataBRFStringB = """CREATE TABLE IF NOT EXISTS brfssdata(
            PrtcpntID   INT NOT NULL PRIMARY KEY AUTO_INCREMENT, 
            STATE       CHAR(2),
            CVDINFR     CHAR,
            CVDCRHD     CHAR,
            CVDSTRK     CHAR,
            CHCCOPD     CHAR,
            DIABETE     CHAR,
            SEX         CHAR,
            RFHYPE      CHAR,
            RFCHOL      CHAR,
            ASTHMS      CHAR,
            RACE        CHAR,
            AGEG5YR     CHAR(2),
            BMI5CAT     CHAR,
            INCOMG      CHAR,
            SMOKER      CHAR,
            RFDRHV      CHAR,
            PACAT       CHAR,
            YEAR        CHAR(4));"""
    try:
        cur.execute(DataBRFStringA)
        cur.execute(DataBRFStringB)
    # If an error occurs executing the queries, then print the error and location.
    except mariadb.Error as e2:
        print('Error creating Table brfssdata: {error}'.format(error=e2))

    cur.close()
    DataUser.CloseConnect()

def UpdateDatabaseQuery(DataTable, DataColumns, DataTableName):

    import math

    DataString = ''
    for DataRow in DataTable:
        RowString = '('
        for DataElement in DataRow:
            if isinstance(DataElement,(float, int)):
                if math.isnan(DataElement):
                    RowString += "' ', "
                else:
                    RowString += str(DataElement) + ", "
            elif isinstance(DataElement,str):
                RowString += "'{}', ".format(DataElement)
        DataString += RowString[:-2] + '),\n'

    DataColumnsString = str(tuple(DataColumns)).replace("'","")
    DatabaseString = 'INSERT INTO {} {} VALUES\n'.format(DataTableName,DataColumnsString)
    DatabaseString += DataString[:-2] + ';'
    return DatabaseString

def ExtractData(DataUser):

    import pandas as pd
    import sys
    import os.path
    import mariadb

    # Descriptions to be used to generate file names for the data sources
    DataFileBase = 'LLCP'
    DataFileRoot = '.ASC'
    DataFilesYearA = ['2014','2016','2018']
    DataFilesYearB = ['2013','2015','2017','2019']
    
    # Blocking variables. The files contain between 400,000 and 500,000 lines.
    # Each line is a record for a survey conducted. Rather than read in all the
    # data at once, the data will be processed in blocks where the number of 
    # records included is BlockSize. max is the maximum number of times the
    # file will be read.
    max = 150
    BlockSize = 5000

    # Loop over the files to be processed. The data is processed for every other
    # year due to the variations in the yearly survey. One factor of interest is
    # the classification of the respondents physical activity. This data is collected
    # in odd years.
    for Year in range(len(DataFilesYearB)):

        # Create the file name and check to see if it exists
        DataFile = DataFileBase + DataFilesYearB[Year] + DataFileRoot
        if (os.path.exists(DataFile)):
            print('Opening file {}'.format(DataFile))

            # Set of lists that contain information about the data columns accessed in the
            # data file: name and location. Name are similar to the names listed in the 
            # documentation provided which explains the files contents. In this case,
            # leading underscores (_) and trailing numbers have been removed to provide
            # consistent names. The exact name of the variables varies year to year.
            # The data files used store the data as ASCII characters with each row holding
            # the record of each survey. The answers to questions and other computed values
            # are stored at specific positions in the row. The structure varies from year
            # to year. The location of data fields is provided in the survey data's 
            # documentation. VarLoc is a list of the first and last column of the data field
            # being extracted. Most of the data fields are one character so the first and
            # last columns are the same. 
            VarList = ['STATE','CVDINFR','CVDCRHD','CVDSTRK','CHCCOPD','DIABETE','SEX','RFHYPE','RFCHOL','ASTHMS',
                       'RACE','AGEG5YR','BMI5CAT','INCOMG','SMOKER','RFDRHV','PACAT']
            VarLoc = [[[1,2],[98,98],[99,99],[100,100],[105,105],[109,109],[178,178],[2103,2103],
                       [2105,2105],[2108,2108],[2173,2173],[2177,2178],[2196,2196],[2200,2200],
                       [2201,2201],[2216,2216],[2348,2348]],
                      [[1,2],[106,106],[107,107],[108,108],[113,113],[117,117],[120,120],[1896,1896],
                       [1898,1898],[1902,1902],[1967,1967],[1971,1972],[1992,1992],[1996,1996],
                       [1997,1997],[2009,2009],[2139,2139]],
                      [[1,2],[106,106],[107,107],[108,108],[113,113],[117,117],[125,125],[1950,1950],
                       [1952,1952],[1956,1956],[2024,2024],[2028,2029],[2049,2049],[2053,2053],
                       [2054,2054],[2068,2068],[2196,2196]],
                      [[1,2],[117,117],[118,118],[119,119],[124,124],[127,127],[91,91],[1903,1903],
                       [1905,1905],[1909,1909],[1976,1976],[1981,1982],[2002,2002],[2006,2006],
                       [2007,2007],[2019,2019],[2101,2101]]]

            # List of three different groups that will be used to clean data after it is loaded.
            # HealthList includes eight medical conditions that are of interest. FactorList
            # includes four lifestyle factors that may be connected to the medical conditions.
            # The purpose of this model is to determine if lifestyle choices such as smoking,
            # heavy drinking, and a lack of physical exercise have any impact on the risk of
            # devloping heart disease, diabetes and respiratory problems. If the connection 
            # exists, can a lifestyle intervention reduce the cost.
            # DemogrList includes respondents location, birthsex, racial, age grouping and income
            # group. These variables can be used account for difference between the sample
            # and population.
            FactorList = ['BMI5CAT','SMOKER','RFDRHV','PACAT']
            HealthList = ['CVDINFR','CVDCRHD','CVDSTRK','CHCCOPD','DIABETE','RFHYPE','RFCHOL','ASTHMS']
            DemogrList = ['STATE','SEX','RACE','AGEG5YR','INCOMG']

            # Loop over each block in the file
            for k in range(max):

                # Counter used to determine which block the file line belongs to.
                # Counter acts as a line number
                j = 0
                print('Run {}'.format(k+1))

                # Open data file
                Dfile_in = open(DataFile,"r")

                # Lists to be used to collect data from the file
                DataList = []
                IndexList = []

                # Loop over each line of the file
                for line in Dfile_in:

                    # Increment counter
                    j += 1

                    # Check if the line is within the current block (k).
                    # If the line is after the current block, exit the
                    # loop over lines in the file. If it is before the
                    # current block, continue to the next line.
                    if j // BlockSize > k: break
                    if j // BlockSize < k: continue

                    # Initialize list for data from each row
                    RowList = []

                    # Loop over the variables in the VariableList.
                    # The location of the first and last character of the 
                    # variable is: 
                    # Varloc[Year index][Variable index][0] and 
                    # Varloc[Year index][Variable index][1].
                    # Append all the characters between the first and last
                    # character inclusive to RowList.
                    for i in range(len(VarList)):
                        RowList.append(line[VarLoc[Year][i][0]-1:VarLoc[Year][i][1]])
                    # Append the RowList to the DataList
                    DataList.append(RowList)
                    # Append the line number (Counter j) to the IndexList
                    IndexList.append(j)

                # Check if there is any data in DataList. If there are not
                # anymore lines in the data file, then exit loop over data blocks.
                if len(DataList) == 0: break

                # Once the DataList and IndexList have been formed and they are not empty. 
                # Create a data frame to hold the information
                HealthDataframe = pd.DataFrame(DataList,columns=VarList,index = IndexList)
                print(HealthDataframe.shape)

                # Clean data
                # 1. Remove US territories
                HealthDataframe['STATE'] = pd.to_numeric(HealthDataframe['STATE'])
                HealthDataframeClean = HealthDataframe.loc[HealthDataframe['STATE'] < 57]
                print('STATE',HealthDataframeClean.shape)

                # 2. Keep only surveys which identify respondent as Male or Female and remove
                #    any others.
                #  '1' - Male
                #  '2' - Female
                SexValues = ['1','2']
                HealthDataframeClean = HealthDataframeClean.loc[HealthDataframe['SEX'].isin(SexValues)]
                print('SEX',HealthDataframeClean.shape)

                # 3. Keep only surveys where age group is identified and remove any where the
                #    age group is not included. Note the leading 0 character in the data string.
                #  '01' - 18 - 24
                #  '02' - 25 - 30
                #  '13' - 80 and up
                AgeValues = ['01','02','03','04','05','06','07','08','09','10','11','12','13']
                HealthDataframeClean = HealthDataframeClean.loc[HealthDataframe['AGEG5YR'].isin(AgeValues)]
                print('AGEG5YR',HealthDataframeClean.shape)

                # 4. Keep only surveys where response include one of the first four numbers (1,2,3,4).
                #    Remove surveys where the data is missing for any reason. Values such as 7, 8, 9, 
                #    and ' ' are to be removed.  
                # 'CVDINFR', 'CVDCRHD', 'CVDSTRK', 'CHCCOPD'
                #   '1' : Yes
                #   '2' : No
                # 'DIABETE'
                #   '1' : Yes
                #   '2' : Yes only during pregnancy
                #   '3' : No
                #   '4' : No borderline
                # 'RFHYPE', 'RFCHOL'
                #   '1' : No
                #   '2' : Yes (Cholesterol or BP checked and medical provider reported it was high) 
                # 'ASTHMS'
                #   '1' : Current
                #   '2' : Former
                #   '3' : Never
                HealthValues = ['1','2','3','4']
                for Var in HealthList:
                    HealthDataframeClean = HealthDataframeClean.loc[HealthDataframeClean[Var].isin(HealthValues)]
                    print(Var,HealthDataframeClean.shape)

                # 5. Keep only surveys where response include one of the first four numbers (1,2,3,4).
                #    Remove surveys where the data is missing for any reason. Values such as 7, 8, 9, 
                #    and ' ' are to be removed.  
                #'BMI5CAT'
                #   '1' : Underweight
                #   '2' : Normal weight
                #   '3' : Overweight
                #   '4' : Obese
                #'SMOKER'
                #   '1' : Current daily smoker
                #   '2' : Current occasional smoker
                #   '3' : Former Smoker
                #   '4' : Never Smoked
                #'RFDRHV' 
                #   '1' : No
                #   '2' : Yes
                #'PACAT'
                #   '1' : Highly active
                #   '2' : active
                #   '3' : Insufficiently active
                #   '4' : Inactive
                FactorValues = ['1','2','3','4']
                for Var in FactorList:
                    HealthDataframeClean = HealthDataframeClean.loc[HealthDataframeClean[Var].isin(FactorValues)]
                    print(Var,HealthDataframeClean.shape)

                # Add YEAR column to dataframe so data can be filtered by year in future
                HealthDataframeClean['Year'] = DataFilesYearB[Year]

                # Transform dataframe to list to be added to database
                # HealthDataColumns is a list of data fields in the database.
                HealthDataColumns = ['STATE','CVDINFR','CVDCRHD','CVDSTRK','CHCCOPD','DIABETE','SEX','RFHYPE','RFCHOL','ASTHMS',
                       'RACE','AGEG5YR','BMI5CAT','INCOMG','SMOKER','RFDRHV','PACAT','YEAR']
                HealthDataList = HealthDataframeClean.values.tolist()

                # If there are no new fields to be added exit loop over blocks. Here the assumption
                # is that there will be at least one record in each block of data. The only exception
                # is the last block which will typically have fewer records and may have them all removed
                # during the data cleaning process.
                if len(HealthDataList) == 0: break

                # Strings for querying database. 
                #   1. Select database with the data table
                #   2. Insert the data into the table
                HealthDataStringA = 'USE healthdata;'
                HealthDataStringB = UpdateDatabaseQuery(HealthDataList,HealthDataColumns,'brfssdata')

                try:
                    # Connect to database. The username and password are contained in DataUser and
                    # used to establish a connection. In other scripts, the connection closed 
                    # prematurely so open and close the connection only when executing a set of queries.
                    DataUser.NewConnect()
                    cur = DataUser.Connection.cursor()
                    cur.execute(HealthDataStringA)
                    cur.execute(HealthDataStringB)
                    cur.close()
                    DataUser.CloseConnect()
                except mariadb.Error as e2:
                    print('Error inserting Data into Table brfssdata: {error}'.format(error=e2))


def UpdateMenu(DataUser):

    if False:
        CreateDatabase(DataUser)
        ExtractData(DataUser)
    #ProfileData(DataUser)
