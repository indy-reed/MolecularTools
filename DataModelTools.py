#
# description:  Set of tools to facilitate automation of data extraction and manage data
# author:       David Price
# location:     7 October 2020
#

import mariadb
import DataGroup
import MolecularModelData
import HealthModelData
import HealthDatabase
import MolecularDatabase
import CompareMethods
import UpdateStatus

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
        """
            This is the class constructor. It requests the username and password.
            If a connection to the cloud database can be established for the 
            username and password, then it checks whether the user has the 
            priviledge to create the database or insert new data. This priviledge
            will open additional options in the main menu. The user will have
            three oportunities to enter their username and password. If they
            are unable to log on, the program will exit.
            Warning: the existing database is dropped when a new database is created.
            This is a time consuming process. You have been warned.
        """

        # Standard tools that allow a user to enter a password without any text
        # displayed on the screen.
        import getpass

        # The user will have three opportunities to enter their username and password.
        # If successful, a boolean variable is set to true and the loop exits.
        # After three unsuccessful attempts the program will continue with the boolean
        # variable set to False.
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
            # Exception to catch the error thrown if the user is unable to connect 
            # to the database. 
            except mariadb.Error as e:
                print('Attempt {}'.format(i))
                print('Unable connect. Username and password combination not recognized.')
                print('Please try again.')
                self.Connected = False
        # After the user connection is verified, check the privileges the user has
        # for the database. For now, does the user have privilege to create a new
        # databases? If so, the program will provide additional options that will
        # allow the user to create the database and insert new data.
        # Warning: With this privilege, the user can reconstruct the entire database.
        # Use create option with caution.
        if self.Connected:
            AccessQuery = Connection.cursor()
            AccessQuery.execute('USE mysql')
            AccessQuery.execute("SELECT Create_priv FROM user WHERE User = '{}'".format(self.Name))
            AccessList = [i for sub in AccessQuery for i in sub]
            if (AccessList[0] == 'Y'):
                self.Update = True
            AccessQuery.close()
            Connection.close()
        # If the user is unable to connect, then the program warns the user that
        # the number of logins has been exceeded.
        else:
            print('Maximum number of login attempts exceeded.')

    def test(self):
        """
            This attribute tests the connection to the database. 
            It is mostly used in debugging. For now, there is 
            an option in the main menu to test the connection.
            If the attribute can connect to the database and
            query the list of tables, it will return the list
            of tables in the database. If a mariadb exception
            occurs, it will return an empty list.
        """
        # Establish connection and execute a query that returns
        # a table with the names of each table in the database.
        # Close the connection. Convert the table to a list and
        # return it.
        try:
            Connection = mariadb.connect(
                                    user = self.Name,
                                    host = self.Host,
                                    password= self.Password,
                                    port=3306)
            TestQuery = Connection.cursor()
            TestQuery.execute('USE moleculardata')
            TestQuery.execute('SHOW TABLES')
            TestQuery.close()
            Connection.close()
            return [j for sub in TestQuery for j in sub]
        # Exception to catch database errors. Exceptions could include:
        # problem connecting to the database or errors in the data query
        # request. 
        # Returns an empty list.
        except mariadb.Error as e:
            print('Unable open connection {}.'.format(e))
            return[]

    def OpenConnection(self):
        """
            This atribute was developed to resolve a connection issue.
            Originally, construtor openned a connection to the database 
            that could be accessed at any time. A subroutine could open
            a cursor for querying the data with the connection.
            But, when processing large quanities of information,
            the connection dropped.
            To resolve this issue, a new connection is opened and closed 
            in each subroutine that queries data from the database. 
            The connection is opened using the user name and password
            stored in the user object.
        """
        # Open connection to database. If the database is not accessible,
        # throw a mariadb exception.
        try: 
            Connection = mariadb.connect(
                                    user = self.Name,
                                    host = self.Host,
                                    password= self.Password,
                                    port=3306)
        # Catch mariadb exception.
        except mariadb.Error as e:
            print('Unable open connection {}.'.format(e))

        return Connection

    def CloseConnection(self,Connection):
        """
            Close the connection.
        """
        Connection.close()

def MolecularToolsStartUp():
    """
        Text banner displayed when starting the program/script.
    """
    StartUpLine1 = 'Welcome to Data Tools'
    StartUpLine2 = 'The software to support Data Science' 
    StartUpLine3 = 'for data collected from CFOUR'
    StartUpHeader = '\t+{:-<42}+\n'.format('')
    StartUpHeader += '\t| {:^40} |\n'.format(StartUpLine1)
    StartUpHeader += '\t+{:-<42}+\n'.format('')
    StartUpHeader += '\t| {:40} |\n'.format(StartUpLine2) 
    StartUpHeader += '\t| {:40} |\n'.format(StartUpLine3) 
    StartUpHeader += '\t+{:-<42}+\n'.format('')
    print(StartUpHeader)

def MolecularToolsMainMenu(DataUser):
    """
        Menu and Prompt screens for menu driven user interface
        Main Menu String
            Current Options:
            A - Update Status:  provides user with what the number of compounds in 
                                a group that have been completed for a particular
                                level of theory and basis set combination.
            B - Compare Methods: allows user to compare various computed properties
                                such as energy and coordinates for different methods
              - Query Data:     pulls data from database for later use.
            C - Model Data:     uses machine learning to construct a data model using
                                neural networks.
            D - Test Query:     Test connection to database.
            U - Update Data:    Allows users with the priviledge to create or insert
                                data the option to do so.
    """

    import MenuTools

    MMName = 'MAIN MENU'
    MMOptions = [['A','Update Status'],['B','Compare Methods'],['C','Model Molecular Data'],
                 ['D','Model Health Data'],
                  ['Q','Quit']]
    if DataUser.Update:
        MMOptions.append(['U','Update Molecule Database'])
        MMOptions.append(['V','Update Health Database'])
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
            UpdateStatus.UpdateStatus(DataUser)
        elif Option == 'B':
            CompareMethods.CompareMethods(DataUser)
        elif Option == 'C':
            MolecularModelData.ModelData(DataUser)
        elif Option == 'D':
            HealthModelData.ModelData(DataUser)
        elif (Option == 'Q') or (Option == 'q'):
            print('Exiting Data Tools')
        elif (Option == 'U') and DataUser.Update:
            MolecularDatabase.UpdateMenu(DataUser)
        elif (Option == 'V') and DataUser.Update:
            HealthDatabase.UpdateMenu(DataUser)
        # if the user does not select a valid option, a warning is
        # printed and the menu and command prompt is reprinted.
        else:
            print('Input not recognized.')

MolecularToolsStartUp()
DataUser = User()
MolecularToolsMainMenu(DataUser)


