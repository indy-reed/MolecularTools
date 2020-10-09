#
# description:  Set of tools to facilitate automation of data extraction and manage data
# author:       David Price
# location:     7 October 2020
#

import mariadb

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

    Groups = ['nano','small','medium','large','macro']
    for Group in Groups:
        print('Summary of {} molecule group:'.format(Group))
        try:
            StatusQuery = DataUser.Connection.cursor()
            StatusQuery.execute('USE molecules')
            StatusQuery.execute("""SELECT c.CalcType, c.BasisSet, COUNT(*) AS count 
                                FROM calculations AS c 
                                JOIN molecules AS m 
                                ON c.MoleID = m.MoleID
                                GROUP BY c.CalcType, c.BasisSet""")
            StatusList = [j for sub in StatusQuery for j in sub]
            print(StatusList)
        except mariadb.Error as e:
            print('Error {} in UpdateStatus Group {}'.format(e,Group))
    Pause = input('Press any key to return to Main Menu.')

def CompareMethods(DataUser):

    GrpName = 'small'
    print('Comparison of {} molecule group:'.format(GrpName))

    Pause = input('Press any key to return to Main Menu.')

def CreateMenu(Title,Options):
    MenuString =  '\t+{:-<36}+\n'.format('')
    MenuString += '\t|{:^36}|\n'.format(Title)
    MenuString += '\t+{:-<36}+\n'.format('')
    MenuString += '\t|{:^12}|{:^23}|\n'.format('Options','Data Task')
    MenuString += '\t+{:-<12}+{:-<23}+\n'.format('','')
    for Option in Options:
        MenuString += '\t|{:^12}| {:22}|\n'.format(Option[0],Option[1])
    MenuString += '\t+{:-<12}+{:-<23}+\n'.format('','')
    return MenuString

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
    MMName = 'MAIN MENU'
    MMOptions = [['A','Update Status'],['B','Compare Methods'],['C','Test Database'],
                  ['Q','Quit']]
    MMString = CreateMenu(MMName,MMOptions)

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
