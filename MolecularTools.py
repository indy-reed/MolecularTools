#
# description:  Set of tools to facilitate automation of data extraction and manage data
# author:       David Price
# location:     7 October 2020
#

import MolecularDatabase
import mariadb
import DataGroup

class User:

    """ 
        This object is intended to allow a user to connect to a Maria database.
        It includes the username, password and hostname. When it is initiated,
        it prompts the user to enter a user name and password. Then, it tries
        to create a connection. If a connection is made, it can be accessed by
        self.Connection. If a connection is not made, any attempt to access 
        self.Connection's attributes will throw an AttributeError. 
        For convience, a boolean variable Connected will indicate if a connection
        is established. It is set to True by default and is changed to False if
        an attempt to connect to a database fails.
        To test the connection, a test function will return a list of the tables
        currently in the database.
    """

    import mariadb
    
    # Object constructor
    def __init__(self):

        # Standard tools that allow a user to enter a password without any text
        # displayed on the screen.
        import getpass

        for i in range(3):
            # Gets user information. For now, host is fixed.
            self.Name = input('login as: ')
            self.Password = getpass.getpass('Password: ')
            self.Host = 'ideasdba.mariadb.database.azure.com'

            # Variable to indicate what permissions the user has. If the user
            # can insert data or create tables, then it is set to True and the
            # user can access tools that create the database and update data.
            # If the user has not been granted these permissions, then the user
            # is limited to the tools for querying data and creating data models.
            self.Update = False

            # Variable to indicate whether the user is connected to the database
            self.Connected = True
            try: 
                self.Connection = mariadb.connect(
                                    user = self.Name,
                                    host = self.Host,
                                    password= self.Password,
                                    port=3306)
                break
            except mariadb.Error as e:
                print('Attempt {}'.format(i))
                print('Unable connect. Username and password combination not recognized.')
                print('Please try again.')
                self.Connected = False
        if self.Connected:
            AccessQuery = self.Connection.cursor()
            AccessQuery.execute('USE mysql')
            AccessQuery.execute("SELECT Create_priv FROM user WHERE User = '{}'".format(self.Name))
            AccessList = [i for sub in AccessQuery for i in sub]
            if (AccessList[0] == 'Y'):
                self.Update = True

        if not self.Connected:
            print('Maximum number of login attempts exceeded.')

    def __del__(self):
        if self.Connected:
            self.Connection.close()

    def test(self):
        TestQuery = self.Connection.cursor()
        TestQuery.execute('USE molecules')
        TestQuery.execute('SHOW TABLES')
        return [j for sub in TestQuery for j in sub]

def MolecularToolsStartUp():
    StartUpLine1 = 'Welcome to Molecular Tools'
    StartUpLine2 = 'The software to support Data Science' 
    StartUpLine3 = 'for data collected from CFOUR'
    StartUpHeader = '\t+{:-<42}+\n'.format('')
    StartUpHeader += '\t| {:^40} |\n'.format(StartUpLine1)
    StartUpHeader += '\t+{:-<42}+\n'.format('')
    StartUpHeader += '\t| {:40} |\n'.format(StartUpLine2) 
    StartUpHeader += '\t| {:40} |\n'.format(StartUpLine3) 
    StartUpHeader += '\t+{:-<42}+\n'.format('')
    print(StartUpHeader)

def UpdateStatus(DataUser):

    import pandas as pd;

    Groups = ['nano','small','medium','large','macro']
    BasisDict = {'pvdz': 'cc-pVDZ', 'pvtz': 'cc-pVTZ', 'pvqz': 'cc-pVQZ',
                 'pcvdz':'cc-pCVDZ','pcvtz':'cc-pCVTZ','pcvqz':'cc-pCVQZ'}
    CalcDict = {'scf':'SCF','mp2':'MP2','ccsd':'CCSD','ccsdt':'CCSD(T)'}
    for Group in Groups:
        #print('Summary of {} molecule group:'.format(Group))
        try:
            StatusQuery = DataUser.Connection.cursor()
            StatusQuery.execute('USE moleculardata')
            StatusQuery.execute("""SELECT c.CalcType, c.BasisSet, COUNT(*) AS count 
                                FROM calculations AS c 
                                JOIN molecules AS m 
                                ON c.MoleID = m.MoleID
                                WHERE m.MoleGroup = '{GROUP}'
                                GROUP BY c.CalcType, c.BasisSet""".format(GROUP=Group))
            StatusList = StatusQuery.fetchall()
        except mariadb.Error as e:
            print('Error {} in UpdateStatus Group {}'.format(e,Group))
            StatusList = []
        if len(StatusList) > 0:
            StatusColumns = ['calc','basis','count']
            StatusDf = pd.DataFrame(StatusList,columns=StatusColumns)
            StatusPT = pd.pivot_table(StatusDf,index = 'calc',columns='basis')
            BasisList = StatusPT.columns.values.tolist()
            CalcList = StatusPT.index.values.tolist()
            CountList = StatusPT.values.tolist()
            StatusStringA = '+{:-<10}'.format('')
            StatusStringB = '|{:^10}'.format(Group)
            for Basis in BasisList:
                StatusStringA += '+{:-<10}'.format('')
                StatusStringB += '|{:^10}'.format(BasisDict[Basis[1]])
            StatusString = StatusStringA + '+\n' + StatusStringB + '|\n' + StatusStringA + '+\n'
            for i in range(len(CountList)):
                StatusLine = '|{:^10}'.format(CalcDict[CalcList[i]])
                for j in range(len(CountList[i])):
                    StatusLine += '|{:^10}'.format(CountList[i][j])
                StatusString += StatusLine + '|\n'
            StatusString += StatusStringA + '+\n'
            print(StatusString)

    Pause = input('Press any key to return to Main Menu.')

def GroupMenuOption(Reference='',DataSets=['nano','small','medium','large','macro']):

    MenuString = '\t+{:-<30}+\n\t|{:^30}|\n\t+{:-<30}+\n'.format('',Reference.upper()+' MENU','')
    MenuString += '\t|{:^10}|{:^19}|\n\t|{:-<10}+{:-<19}|\n'.format('Option','Data Set','','')
    for i in range(len(DataSets)):
        MenuString += '\t|{:^10}| {:<18}|\n'.format(str(i+1),DataSets[i])
    MenuString += '\t+{:-<30}+'.format('')
    print(MenuString)

    OptionPromptString = 'Please select a data set for {}: '.format(Reference)

    OptionS = input(OptionPromptString)
    if OptionS.isdigit():
        Option = int(OptionS)
        if (Option > 0) and (Option <= len(DataSets)):
            return Option - 1
        else:
            print ('Option is not recognized. Option set to default.')
            return len(DataSets) - 1
    else:
        print ('Input is not recognized. Option set to default.')
        return len(DataSets) - 1

def GetRotationalData(DataUser,CompGroup,CoordLabel):

    import pandas as pd

    # Query the data base for rotational constants
    CompareQuery = DataUser.Connection.cursor()
    CompareQuery.execute('USE moleculardata')
    CompareQuery.execute("""SELECT M.MoleLabel, CONCAT(C.CalcType, '/' ,C.BasisSet) AS CB, 
                                C.RotConst{}
                                FROM calculations AS C
                                JOIN molecules AS M
                                    ON C.MoleID = M.MoleID""".format(CoordLabel))
    BData = CompareQuery.fetchall()
    CompareQuery.close()

    # Transform data to dataframe
    BColumns = ['MoleLabel','calc/basis','B']
    BDataframe = pd.DataFrame(BData,columns=BColumns)

    # Create pivot table:
    # Dataframe:
    # Label   calc/basis  B
    #   A1       B1      C1
    #   A1       B2      C2
    #   A2       B1      C3
    #   A2       B2      C4
    #    pivot table
    #           B1       B2
    #   A1      C1       C2
    #   A2      C3       C4
    BPivotTable = pd.pivot(BDataframe,index='MoleLabel',columns='calc/basis')

    # Clean pivot table by:
    # 1. Select the calculation/basis set combinations associated with the data
    #    the user wants to compare
    BPivotTable = BPivotTable[CompGroup.PivotComboList('B')]
    # 2. Remove any rows where the rotational constant is NaN
    BPivotTable.dropna(axis=0,how='any',inplace=True)

    return BPivotTable

def CompareMethods(DataUser):

    import pandas as pd

    import DataGroup

    CI95 = 1.96
    # Obtain user option which corresponds to the index in Groups 

    Groups = ['nano','small','medium','large']
    A = GroupMenuOption(Reference='Calculation Comparison',DataSets=Groups)
    CompareGroup = DataGroup.DataGroup(A)

    BxyzPivotTable = pd.DataFrame()
    for Coord in ['X','Y','Z']:
        BPivotTable = GetRotationalData(DataUser,CompareGroup,Coord)
        BxyzPivotTable = BxyzPivotTable.append(BPivotTable)

    # Compute the relative difference of the rotational constant with respect to
    # the reference for the data set.
    #
    #    (Bx - Bx,r) / Bx,r
    BxyzPivotRef = BxyzPivotTable[CompareGroup.RefComboTuple('B')]
    BxyzPivotTable = BxyzPivotTable.sub(BxyzPivotRef,axis=0)
    BxyzPivotTable = BxyzPivotTable.div(BxyzPivotRef,axis=0)

    # Compute the mean and standard deviation of the realative difference for
    # each calculation/ basis set combination
    MeanBxyz = BxyzPivotTable.mean(axis=0).values.tolist()
    StDvBxyz = BxyzPivotTable.std(axis=0).values.tolist()
    SkewBxyz = BxyzPivotTable.skew(axis=0).values.tolist()

    # Print summary table with the mean, standard deviation (Std Dv) and confidence interval (95%)
    # The confidence interval is 1.96 times the standard of deviation.
    ComparisonH = '\t+{:-<59}+\n\t|{:^59}|\n\t|{:>20} {:<38}|\n'.format('',                                       
          'Comparison of relative difference of',len(BxyzPivotTable),
          'Rotational Constants')
    ComparisonA = '\t+{:-<20}+{:-<12}+{:-<12}+{:-<12}+\n'.format('','','','')
    ComparisonB = '\t|{:^20}|{:^12}|{:^12}|{:^12}|\n'.format(CompareGroup.Name(),
                                                             'Mean','Std Dv','95% CI')
    ComparisonD = ''
    GpOut = CompareGroup.PrintComboList()
    for i in range(len(GpOut)):
        ComparisonD += '\t|{:^20}|{:^12.6f}|{:^12.6f}|{:^12.6f}|\n'.format(GpOut[i],
                                              MeanBxyz[i],StDvBxyz[i],CI95*StDvBxyz[i])
    Comparison = ComparisonH + ComparisonA + ComparisonB + ComparisonA + ComparisonD + ComparisonA
    print (Comparison)

    Pause = input('Press <Enter> to return to Main Menu.')


def MolecularToolsMainMenu(DataUser):
    # Menu and Prompt screens for menu driven user interface
    # Main Menu String
    #   Current Options:
    #       A - Update Status:  provides user with what the number of compounds in 
    #                           a group that have been completed for a particular
    #                           level of theory and basis set combination.
    #       B - Compare Methods: allows user to compare various computed properties
    #                           such as energy and coordinates for different methods
    #         - Query Data:     pulls data from database for later use.
    #         - Model Data:     uses machine learning to construct a data model using
    #                           neural networks.
    #       C - Test Query:     Test connection to database.
    #

    import MenuTools

    MMName = 'MAIN MENU'
    MMOptions = [['A','Update Status'],['B','Compare Methods'],['C','Test Database'],
                  ['Q','Quit']]
    if DataUser.Update:
        MMOptions.append(['U','Update Database'])
    MMString = MenuTools.CreateMenu(MMName,MMOptions)

    # User prompt string connected to Main Menu
    PromptString = 'Please select an option from Main Menu: '

    # Control code for Main Menu
    Option = ''
    while (Option != 'Q') and (Option != 'q'):
        # Output menu and prompt
        print(MMString)
        Option = input(PromptString)
        # Option A: Update status
        if Option == 'A':
            UpdateStatus(DataUser)
        elif Option == 'B':
            CompareMethods(DataUser)
        elif Option == 'C':
            print(DataUser.test())
        elif (Option == 'Q') or (Option == 'q'):
            print('Exiting Data Tools')
        elif (Option == 'U') and DataUser.Update:
            MolecularDatabase.UpdateMenu(DataUser)
        # if the user does not select a valid option, a warning is
        # printed and the menu and command prompt is reprinted.
        else:
            print('Input not recognized.')

MolecularToolsStartUp()
DataUser = User()
if not DataUser.Connected:
    del DataUser
    exit()

MolecularToolsMainMenu(DataUser)
del DataUser

