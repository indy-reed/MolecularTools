#
# description:  Set of tools to facilitate automation of data extraction and manage data
# author:       David Price
# location:     7 October 2020
#

import MolecularDatabase
import mariadb
import DataGroup
import CompareMethods

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
    
    # The constructor will get the user's name and password. Then, test the 
    # connection to the database. If a connection can be established, it will
    # access the user profile and determine if the user can update the database.
    # The user has permission to update the database, connect to the database,
    # user name and password are then stored for future use. The test connection
    # is closed. 
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
                Connection = mariadb.connect(
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
            AccessQuery = Connection.cursor()
            AccessQuery.execute('USE mysql')
            AccessQuery.execute("SELECT Create_priv FROM user WHERE User = '{}'".format(self.Name))
            AccessList = [i for sub in AccessQuery for i in sub]
            if (AccessList[0] == 'Y'):
                self.Update = True
            AccessQuery.close()
            Connection.close()

        if not self.Connected:
            print('Maximum number of login attempts exceeded.')

    def test(self):

        try:
            Connection = mariadb.connect(
                                    user = self.Name,
                                    host = self.Host,
                                    password= self.Password,
                                    port=3306)
            TestQuery = Connection.cursor()
            TestQuery.execute('USE moleculardata')
            TestQuery.execute('SHOW TABLES')
            return [j for sub in TestQuery for j in sub]
            TestQuery.close()
            Connection.close()
        except mariadb.Error as e:
            print('Unable open connection {}.'.format(e))
            return[]

    def OpenConnection(self):

        try: 
            Connection = mariadb.connect(
                                    user = self.Name,
                                    host = self.Host,
                                    password= self.Password,
                                    port=3306)
        except mariadb.Error as e:
            print('Unable open connection {}.'.format(e))

        return Connection

    def CloseConnection(self,Connection):
        Connection.close()

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
    try:
        StatusConnection = DataUser.OpenConnection()
        for Group in Groups:
            try:    
                StatusQuery = StatusConnection.cursor()
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

        DataUser.CloseConnection(StatusConnection)
    except mariadb.Error as ea:
        print('Error connection to data serve: {}.'.format(ea,Group))

    Pause = input('Press any key to return to Main Menu.')


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
            CompareMethods.CompareMethods(DataUser)
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


MolecularToolsMainMenu(DataUser)


