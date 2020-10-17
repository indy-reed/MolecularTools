def CreateDatabase(DataUser):

    """
        This subroutine creates the database that will be used by MolecularTools.
        It drops the existing database and all the data tables. If it is run,
        all the data that is currently included in the tables will be lost.
        The option to access this routine from the menu is restricted to users
        that have Create_priv on the server.
    """
    import mariadb

    # Open cursor for data queries using the Connection in DataUser
    cur = DataUser.Connection.cursor()

    # Map of relational database A
    MDCompString =  '\t+{:-<22}+\n'.format('')
    MDCompString += '\t| {:^20} |\n'.format('moleculardata')
    MDCompString += '\t| {:^20} |\n'.format('Database')
    MDCompString += '\t+{:-<22}+\n'.format('')
    print(MDCompString)
    # Create database moleculardata
    DatabaseIntStringA = 'DROP DATABASE IF EXISTS moleculardata;'
    DatabaseIntStringB = 'CREATE DATABASE IF NOT EXISTS moleculardata;'
    DatabaseIntStringC = 'USE moleculardata;'
    try:
        cur.execute(DatabaseIntStringA)
        cur.execute(DatabaseIntStringB)
        cur.execute(DatabaseIntStringC)
    # If an error occurs executing the queries, then print the error and location.
    except mariadb.Error as e1:
        print('Error creating Database moleculardata: {error}'.format(error=e1))

    # Create table elements
    # elements consists of the first 20 elements of the periodic table. The
    # calculations will only include elements from rows one through three of 
    # the periodic table: specifically: H, B, C, N, O, F, Al, Si, P, S, and Cl.
    # Elements such as He and Na are included for completeness.
    # The Atomic Number represents the primary key. Other information includes
    # Atomic Symbol, name, average atomic mass (in AMU) and mass of most common
    # isotope (in AMU).
    # The table also includes a ghost atom which is used to create a reference
    # point in calculations and can pad the molecular date when coordinates are
    # used in machine learning.
    # (Source of atomic mass and isotope mass)
    DatabaseEleStringA = 'DROP TABLE IF EXISTS elements'
    DatabaseEleStringB = '''CREATE TABLE IF NOT EXISTS elements(
            AtomicNumber INT NOT NULL PRIMARY KEY, 
            AtomicSymbol CHAR(2), 
            AtomName CHAR(20), 
            AtomicMass REAL NOT NULL,
            IsotopeMass  REAL NOT NULL);'''
    # Populate table elements
    DatabaseEleStringC = """INSERT INTO elements(AtomicNumber, AtomicSymbol, AtomName, 
            AtomicMass, IsotopeMass)
            VALUES
            ( 0, 'X',  'ghost atom',  0.00000,      0.00000),
            ( 1, 'H',  'hydrogen',    1.00798,      1.00783),
            ( 2, 'He', 'helium',      4.002602,     4.00260),
            ( 3, 'Li', 'lithium',     6.9675,       6.9675),
            ( 4, 'Be', 'beryllium',   9.0121831,    9.0121),
            ( 5, 'B',  'boron',      10.8135,       11.00931),
            ( 6, 'C',  'carbon',     12.0106,       12.00000),
            ( 7, 'N',  'nitrogen',   14.00686,      14.00307),
            ( 8, 'O',  'oxygen',     15.9994,       15.99491),
            ( 9, 'F',  'fluorine',   18.998403,     18.99840),
            (10, 'Ne', 'neon',       20.1797,       20.1797),
            (11, 'Na', 'sodium',     22.98976928,   22.9897),
            (12, 'Mg', 'magnesium',  24.3055,       24.3055),
            (13, 'Al', 'aluminium',  26.9815385,    26.9815),
            (14, 'Si', 'silicon',    28.085,        28.085),
            (15, 'P',  'phosphorus', 30.973761998,  30.9737),
            (16, 'S',  'sulfur',     32.0675,       31.97207),
            (17, 'Cl', 'chlorine',   35.4515,       34.96885),
            (18, 'Ar', 'argon',      39.948,        39.948),
            (19, 'K',  'potassium',  39.0983,       39.0983),
            (20, 'Ca', 'calcium',    40.078,        40.078);"""
    try:
        cur.execute(DatabaseEleStringA)
        cur.execute(DatabaseEleStringB)
        cur.execute(DatabaseEleStringC)
    # If an error (or exception) occurs executing the queries, then print the error 
    # and location.
    except mariadb.Error as e2:
        print('Error creating Table elements: {error}'.format(error=e2))

    # Map of relational database B
    ETCompString =  '\t+{:-<22}+\n'.format('')
    ETCompString += '\t| {:^20} |\n'.format('elements')
    ETCompString += '\t+{:-<22}+\n'.format('')
    ETCompString += '\t| {:^16}{:>4} |<{:-<13}\n'.format('AtomicNumber','PK','')
    ETCompString += '\t+{:-<22}+{:>15}\n\t{:>39}'.format('','|','|')
    print(ETCompString)

    # Create table molecules. This table will contain common information about
    # each molecule in the database. Information includes: 
    #       PubChem Compound ID: to connect the data to NIH's Pub Chem website/database.
    #       MoleGroup: molecules will be grouped by size and symmetry which will
    #                  be used to indicate which calculations have been included
    #                  in the database.
    #       MoleLabel: a unique combination of a compounds Molecular formula and a
    #                  combination of letters and numbers to indicate the isomer
    #                  and conformation of the structure. In general, capital letters
    #                  will be used for different isomers and numbers will be used
    #                  for 'unique' rotational conformations.
    #       MoleFormula: standard way of identifying different compounds by the type
    #                    of atoms in the molecule and their number. (Source PubChem)
    #       MoleName: primary name listed on PubChem.
    #       SMILES: two dimensional structure from PubChem that indicates how atoms
    #               are connected. In principle, this combination is unique to each
    #               isomer.
    #       JMOL: whether a 3D plot of the structure is available. Plots created by
    #             JMOL.
    #       LCSS, Flammable, AcuteToxicity, Corrosive, Irritant: properties from PubChem.
    #             LCSS is safety sheet for compounds with other properties indicated.
    #       Cyclic: whether the molecule has a ring or not. This is a simple property
    #               and will be used as a classifier.
    DatabaseMolStringA = 'DROP TABLE IF EXISTS molecules;'
    DatabaseMolStringB = """CREATE TABLE molecules(
            MoleID INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
            PubChemCID INT UNSIGNED,
            MoleGroup CHAR(10),
            MoleLabel CHAR(20),
            MoleFormula CHAR(20),
            MoleName CHAR(40),
            SMILES CHAR(100),
            JMOL CHAR,
            LCSS CHAR,
            Flammable CHAR,	
            AcuteToxicity CHAR,
            Corrosive CHAR,
            Irritant CHAR,
            Cyclic CHAR);"""
    try:
        cur.execute(DatabaseMolStringA)
        cur.execute(DatabaseMolStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e3:
        print('Error creating Table molecules: {error}'.format(error=e3))

    # Map of relational database C
    MTCompString =  '\t+{:-<22}+{:>15}\n'.format('','|')
    MTCompString += '\t| {:^20} |{:>15}\n'.format('molecules','|')
    MTCompString += '\t+{:-<22}+{:>15}\n'.format('','|')
    MTCompString += '\t| {:^16}{:>4} |<{:-<8}{:>6}\n'.format('MoleID','PK','','|')
    MTCompString += '\t+{:-<22}+{:>5}{:>5}{:>5}\n\t{:>29}{:>5}{:>5}'.format('','|','|','|','|','|','|')
    print(MTCompString)        

    # Create table calculations. This table contains information about what
    # calculations are included in the data base. For now, all calculations
    # have been performed by CFOUR.
    #   CalcType: Type of calculation method which includes SCF, MP2, CCSD, and CCSD(T)
    #   BasisSet: Basis set used in calculation. Basis sets include p-VXZ and p-CVXZ where
    #             X - D, T and Q.
    #   PntGrp: MolePntGrp and CompPntGrp are include in CFOUR and used to decrease the
    #           time needed for calculations.
    #   NumBasFunc: Number of basis functions in calculation.
    #   SCFEnergy: SCF energy for the molecular structure. Is included for each calculation.
    #              Reported in Hartrees (or atomic units)
    #   MP2Energy, CCSDEnergy, CCSDTEnergy: Energy of structure for particular method/theory.
    #       Note: these methods add a correction to SCF energy and number reported includes
    #       SCF energy and correction. Reported in Hartrees (or atomic units)
    #   RotConstX, RotConstY, RotConstZ: Rotational constants. These represent rotation about
    #       principle axis of rotation for compounds. Reported in MHz.
    #   Calc Time: time calculation took. In this case, data is being pulled from first order
    #              properties on an optimized geometry. 
    #   MoleID: key connecting to calculations to molecules table
    DatabaseCalcStringA = 'DROP TABLE IF EXISTS calculations;'
    DatabaseCalcStringB = """CREATE TABLE IF NOT EXISTS calculations (
            CalcID      INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
            CalcType    CHAR(8),
            BasisSet    CHAR(6),
            MolePntGrp  CHAR(6),
            CompPntGrp  CHAR(6),
            NumBasFunc  INT UNSIGNED NOT NULL,
            SCFEnergy   REAL NOT NULL,
            MP2Energy   REAL,
            CCSDEnergy  REAL,
            CCSDTEnergy REAL,
            RotConstX   REAL,
            RotConstY   REAL,
            RotConstZ   REAL,
            CalcTime    REAL NOT NULL,
            MoleID      INT UNSIGNED NOT NULL,
            CONSTRAINT `fk_mole_cals`
            FOREIGN KEY (MoleID) REFERENCES molecules (MoleID)
                ON DELETE CASCADE
                ON UPDATE RESTRICT);"""
    try:
        cur.execute(DatabaseCalcStringA)
        cur.execute(DatabaseCalcStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e4:
        print('Error creating Table calculations: {error}'.format(error=e4))

    # Map of relational database D
    CTCompString =  '\t+{:-<22}+{:>5}{:>5}{:>5}\n'.format('','|','|','|')
    CTCompString += '\t| {:^20} |{:>5}{:>5}{:>5}\n'.format('calculations','|','|','|')
    CTCompString += '\t+{:-<22}+{:>5}{:>5}{:>5}\n'.format('','|','|','|')
    CTCompString += '\t| {:^16}{:>4} |{:-<4}{:>6}{:>5}\n'.format('MoleID','SK','','|','|')
    CTCompString += '\t| {:^16}{:>4} |<{:-<18}\n'.format('CalcID','PK','')
    CTCompString += '\t+{:-<22}+{:>10}{:>5}{:>5}\n\t{:>34}{:>5}{:>5}'.format('','|','|','|','|','|','|')
    print(CTCompString)        

    # Create table atomsinmolecule. This table is for book keeping of each atom
    # in a molecule. It has the atomic number and a counter which indicates the
    # atom in molecule MoleID.
    DatabaseAIMStringA = 'DROP TABLE IF EXISTS atomsinmolecules;'
    DatabaseAIMStringB = """CREATE TABLE IF NOT EXISTS atomsinmolecules (
            AtomID      INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
            AtomicNum   INT NOT NULL,
            AtomMolNum  INT NOT NULL,
            MoleID      INT UNSIGNED NOT NULL,
            CONSTRAINT `fk_elem_atom`
                FOREIGN KEY (AtomicNum) REFERENCES elements (AtomicNumber)
                ON DELETE CASCADE
                ON UPDATE RESTRICT,
            CONSTRAINT `fk_mole_atom`
                FOREIGN KEY (MoleID) REFERENCES molecules (MoleID)
                ON DELETE CASCADE
                ON UPDATE RESTRICT);"""
    try:
        cur.execute(DatabaseAIMStringA)
        cur.execute(DatabaseAIMStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e5:
        print('Error creating Table atomsinmolecules: {error}'.format(error=e5))

    # Map of relational database E
    ATCompString =  '\t+{:-<22}+{:>10}{:>5}{:>5}\n'.format('','|','|','|')
    ATCompString += '\t| {:^20} |{:>10}{:>5}{:>5}\n'.format('atomsinmolecules','|','|','|')
    ATCompString += '\t+{:-<22}+{:>10}{:>5}{:>5}\n'.format('','|','|','|')
    ATCompString += '\t| {:^16}{:>4} |{:-<9}{:>6}{:>5}\n'.format('MoleID','SK','','|','|')
    ATCompString += '\t| {:^16}{:>4} |{:-<14}{:>6}\n'.format('AtomNum','SK','','|')
    ATCompString += '\t| {:^16}{:>4} |<{:-<3}{:>16}\n'.format('AtomID','PK','','|')
    ATCompString += '\t+{:-<22}+{:>5}{:>15}\n\t{:>29}{:>15}'.format('','|','|','|','|')
    print(ATCompString)        

    # Create table cartCoord. This table stores the Cartesian Coordinates for each
    # atom based on its location in the optimized structure for a particular calculation.
    DatabaseCartCStringA = 'DROP TABLE IF EXISTS cartcoords;'
    DatabaseCartCStringB = """CREATE TABLE IF NOT EXISTS cartcoords (
            CoordID     INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
            XCoord      REAL NOT NULL,
            YCoord      REAL NOT NULL,
            ZCoord      REAL NOT NULL,
            AtomID      INT UNSIGNED NOT NULL,
            CalcID      INT UNSIGNED NOT NULL,
            CONSTRAINT `fk_calc_atom`
                FOREIGN KEY (CalcID) REFERENCES calculations (CalcID)
                ON DELETE CASCADE
                ON UPDATE RESTRICT,
            CONSTRAINT `fk_coord_atom`
                FOREIGN KEY (AtomID) REFERENCES atomsinmolecules (AtomID)
                ON DELETE CASCADE
                ON UPDATE RESTRICT);"""
    try:
        cur.execute(DatabaseCartCStringA)
        cur.execute(DatabaseCartCStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e6:
        print('Error creating Table cartcoords: {error}'.format(error=e6))

    # Map of relational database F
    CCTCompString =  '\t+{:-<22}+{:>5}{:>15}\n'.format('','|','|')
    CCTCompString += '\t| {:^20} |{:>5}{:>15}\n'.format('cardcoords','|','|')
    CCTCompString += '\t+{:-<22}+{:>5}{:>15}\n'.format('','|','|')
    CCTCompString += '\t| {:^16}{:>4} |{:-<4}{:>16}\n'.format('AtomID','SK','','|')
    CCTCompString += '\t| {:^16}{:>4} |{:-<19}\n'.format('CalcID','SK','')
    CCTCompString += '\t| {:^16}{:>4} |\n'.format('CoordID','PK')
    CCTCompString += '\t+{:-<22}+\n'.format('')
    print(CCTCompString)        

    # Create view AtomSummary. Simple view that counts the number of atoms in
    # the molecules in the database.
    DatabaseVSumStringA = 'DROP VIEW IF EXISTS atomsummary;'
    DatabaseVSumStringB = """CREATE VIEW atomsummary AS
            SELECT E.AtomicSymbol, COUNT(*) AS NumAtoms
                FROM molecules AS M
            LEFT JOIN atomsinmolecules AS A
                ON A.MoleID = M.MoleID
            JOIN elements AS E
                ON A.AtomicNum = E.AtomicNumber
            GROUP BY E.AtomicSymbol;"""
    try:
        cur.execute(DatabaseVSumStringA)
        cur.execute(DatabaseVSumStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e7:
        print('Error creating View atomsummary: {error}'.format(error=e7))

    # Create function moleweight. SQL function that can compute the molecular weight
    # for a particular molecule.
    DatabaseFMoleWtStringA = 'DROP FUNCTION IF EXISTS moleweight'
    DatabaseFMoleWtStringB = """CREATE FUNCTION moleweight(MoleID INT)
        RETURNS REAL READS SQL DATA
        BEGIN
            RETURN (SELECT FORMAT(SUM(E.AtomicMass),5)
                        FROM atomsinmolecules AS A
                        JOIN Elements AS E
                            ON A.AtomicNum = E.AtomicNumber
                        WHERE A.MoleID = MoleID);
        END"""
    try:
        cur.execute(DatabaseFMoleWtStringA)
        cur.execute(DatabaseFMoleWtStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e8:
        print('Error creating Function moleweight: {error}'.format(error=e8))

    # Create function molesize. Simple SQL function that determines the number of
    # atoms in each molecule.
    DatabaseFMoleSzStringA = 'DROP FUNCTION IF EXISTS molesize'
    DatabaseFMoleSzStringB = """CREATE FUNCTION molesize(MoleID INT)
        RETURNS INT READS SQL DATA
        BEGIN
            RETURN (SELECT COUNT(*)
                FROM atomsinmolecules AS A
                WHERE A.AtomicNum <> 0
                        AND A.MoleID = MoleID);
        END"""
    try:
        cur.execute(DatabaseFMoleSzStringA)
        cur.execute(DatabaseFMoleSzStringB)
    # If an error occurs executing the queries, then print the error.
    except mariadb.Error as e9:
        print('Error creating Function molesize: {error}'.format(error=e9))

    # Close cursor
    cur.close()

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
    #print(DatabaseString)
    return DatabaseString

def UpdateCalcData(DataUser,MoleculeDataframe,CalcData):

    import mariadb
    import pandas as pd
    
    # Insert new calculation information into calculation table
    #print ('Updating molecular data into TABLE calculations')
    # 1. Query calculations to get current calculations. Each combination of label, method
    #    and basis is unique.
    cur = DataUser.Connection.cursor()
    cur.execute("""SELECT m.MoleLabel, c.CalcType, c.BasisSet
                FROM calculations AS c 
                JOIN molecules AS m
                    ON m.MoleID = c.MoleID""")
    CurrentCalcQuery = cur.fetchall()
    # 2. Convert query to list of current Labels
    CurLabel = list(map(lambda i: '{}:{}/{}'.format(CurrentCalcQuery[i][0],CurrentCalcQuery[i][1],CurrentCalcQuery[i][2]),range(len(CurrentCalcQuery))))
    # 3. Transform calculation data into dataframe 
    CalcDataColumns = ['Label','Comp','CalcType','BasisSet','MolePntGrp','CompPntGrp',
                           'NumBasFunc','SCFEnergy','MP2Energy','CCSDEnergy','CCSDTEnergy',
                           'RotConstX','RotConstY','RotConstZ','CalcTime']
    CalcDataframe = pd.DataFrame(CalcData,columns=CalcDataColumns)
    # 4. Add Secondary Key MoleID.
    cur.execute("SELECT MoleLabel, MoleID FROM molecules")
    MoleIDdict = dict(cur.fetchall())
    MoleIDList = list(map(lambda i: MoleIDdict[CalcData[i][1]],range(len(CalcData))))
    CalcDataframe['MoleID'] = MoleIDList
    CalcDataColumns.append('MoleID')
    # 5. Clean data: remove current molecules currently in the database from data frame.
    #    Drop label and compound columns from dataframe.
    NewCalcDataframe = CalcDataframe[~CalcDataframe['Label'].isin(CurLabel)]
    CalcList = NewCalcDataframe[CalcDataColumns[2:]].values.tolist()
    # 6. If new data exists:
    if len(CalcList) > 0:
        # A. Create string to add data
        DatabaseCalcString = UpdateDatabaseQuery(CalcList,CalcDataColumns[2:],'calculations')
        # B. SQL query
        try:
            cur.execute(DatabaseCalcString)
        except mariadb.Error as e4:
            print('Error testing MariaDB Database Table element: {error}'.format(error=e4))
    cur.close()
    return len(CalcList)
    
def UpdateCoordData(DataUser,MoleculeDataframe,AtomList,CoordList):

    import mariadb
    import pandas as pd

    cur = DataUser.Connection.cursor()
    try:
        cur.execute("SELECT MoleLabel, MoleID FROM molecules")
        MoleIDdict = dict(cur.fetchall())
    except mariadb.Error as e:
        print('Error selecting data from Table molecules: {error}'.format(error=e))

    # Insert new calculation information into atomsInMolecules table
    # 1. Query atomsInMolecule to get current calculations. Each combination of label and 
    #    atom number in molecule (AtomMolNum) is unique.
    #print ('Updating molecular data into TABLE atomsinmolecules.')
    try:
        cur.execute("""SELECT m.MoleLabel, a.AtomMolNum
                FROM atomsinmolecules AS a 
                JOIN molecules AS m
                    ON m.MoleID = a.MoleID""")
        CurrentAIMQuery = cur.fetchall()
    except mariadb.Error as e:
        print('Error joining data from Tables atomsinmolecules and molecules: {error}'.format(error=e))
    # 2. Convert query to list of current Labels in atomsInMolecules
    CurAIMLabel = list(map(lambda i: '{}_{}'.format(CurrentAIMQuery[i][0],CurrentAIMQuery[i][1]),range(len(CurrentAIMQuery))))
    #print(CurAIMLabel)
    # 3. Transform data 
    AIMDataColumns = ['Label', 'Comp', 'AtomicNum', 'AtomMolNum']
    AIMDataframe = pd.DataFrame(AtomList,columns=AIMDataColumns)
    #print(AIMDataframe)
    # 4. Add Secondary Key MoleID.
    AIMMoleIDList = list(map(lambda i: MoleIDdict[AtomList[i][1]],range(len(AtomList))))
    AIMDataframe['MoleID'] = AIMMoleIDList
    AIMDataColumns.append('MoleID')
    #print(AIMDataframe)
    # 5. Clean data: remove current molecules currently in the database from data frame.
    #    Drop label and compound columns from dataframe.
    NewAIMDataframe = AIMDataframe[~AIMDataframe['Label'].isin(CurAIMLabel)]
    #print(NewAIMDataframe)
    AIMList = NewAIMDataframe[AIMDataColumns[2:]].values.tolist()
    # 6. If new data exists:
    if len(AIMList) > 0:
        # A. Create string to add data
        DatabaseAIMString = UpdateDatabaseQuery(AIMList,AIMDataColumns[2:],'atomsinmolecules')
        # B. SQL query
        try:
            cur.execute(DatabaseAIMString)
        except mariadb.Error as e4:
            print('Error inserting into Table atomsinmolecules: {error}'.format(error=e4))
    
    # Insert new calculation information into cartCoord table
    #print ('Updating molecule data into TABLE cartCoord')
    # 1. Query cartCoord to get current calculations. Each combination of compound, calc,basis, 
    #    and atom number in molecule (AtomMolNum) is unique.
    #    Current Coordinate Label: Compound:Calc/basis_atomMolNum
    try:
        cur.execute("""SELECT CONCAT(m.MoleLabel, ':',c.CalcType,'/',c.BasisSet,'_',a.AtomMolNum) AS CurCartLabel
                FROM cartcoords as x
                JOIN atomsinmolecules AS a
                    ON x.AtomID = a.AtomID
                JOIN calculations AS c
                    ON x.CalcID = c.CalcID
                JOIN molecules AS m
                    ON m.MoleID = a.MoleID""")
        CurrentCoordQuery = cur.fetchall()
    except mariadb.Error as e4:
        print('Error joining into Tables: {error}'.format(error=e4))

    CurCoordLabel = [j for sub in CurrentCoordQuery for j in sub]
    #print(len(CurCoordLabel))
    # 2. Create dictionaries to connect labels used to the numeric database keys.
    # Dictionaries for connecting Coordinates to secondary keys (CalcID and AtomID)
    #    A. CalcID dictionary: Desired format of query output: 'compound:calc/basis', CalcID
    try:
        cur.execute("""SELECT CONCAT(m.MoleLabel,':',c.CalcType,'/',c.BasisSet) AS Label, c.CalcID
                FROM calculations AS c 
                JOIN molecules AS m
                    ON m.MoleID = c.MoleID""")
        DictCalcQuery = dict(cur.fetchall())
    except mariadb.Error as e4:
        print('Error joining into Table for Calc Dictionary: {error}'.format(error=e4))

    #    B. AtomID dictionary: Desired format of query output: 'compound_AtomMolNum', AtomID
    try:
        cur.execute("""SELECT CONCAT(m.MoleLabel, '_', a.AtomMolNum) AS Label, a.AtomID
                FROM atomsinmolecules AS a 
                JOIN molecules AS m
                    ON m.MoleID = a.MoleID""")
        DictAIMQuery = dict(cur.fetchall())
    except mariadb.Error as e4:
        print('Error joining into Table for AIM Dictionary: {error}'.format(error=e4))

    # 4. DataList has three labels: 
    #       CalcLabel which will be used to match to CalcID in Table calculations
    #       AIMLabel which will be used to match to AtomMolNum key is Table atomsinmolecules
    #       CoordLabel which will be used to remove any data element already in Table CartCoord
    # Form dataframe from CoordList
    CoordColumns = ['CalcLabel','AIMLabel','CoordLabel','AtomicNum','XCoord','YCoord','ZCoord']
    CoordDataframe = pd.DataFrame(CoordList,columns=CoordColumns)
    #print(len(CoordDataframe))
    # 5. Clean and transform data in CoordDataframe
    #    A. Change type for Atomic Number (int) minor
    CoordTypeDic = {'AtomicNum': int}
    CoordDataframe = CoordDataframe.astype(CoordTypeDic)
    #    B. Insert CalcID -> DictCalcQuery[CalcLabel] into dataframe
    #       Note: for data consistency, remove any data set that does not have
    #       a key in the dictionary.
    DictCalcKey = list(DictCalcQuery.keys())
    #print(CoordDataframe[~CoordDataframe['CalcLabel'].isin(DictCalcKey)])
    CoordDataframe = CoordDataframe[CoordDataframe['CalcLabel'].isin(DictCalcKey)]
    #print(len(CoordDataframe))
    CalcLabelList = CoordDataframe['CalcLabel'].values.tolist()
    CoordDataframe['CalcID'] = [DictCalcQuery[CalcLabelList[i]] for i in range(len(CoordDataframe))]
    #    C. Insert AtomID -> DictAIMQuery[AIMLabel] into dataframe
    #       Note: for data consistency, remove any data set that does not have
    #       a key in the dictionary.
    DictAIMKey = list(DictAIMQuery.keys())
    CoordDataframe = CoordDataframe[CoordDataframe['AIMLabel'].isin(DictAIMKey)]
    #print(len(CoordDataframe))
    AIMLabelList = CoordDataframe['AIMLabel'].values.tolist()
    CoordDataframe['AtomID'] = [DictAIMQuery[AIMLabelList[i]] for i in range(len(CoordDataframe))]
    #    E. Drop all atoms currently in the database
    NewCartDataframe = CoordDataframe[~CoordDataframe['CoordLabel'].isin(CurCoordLabel)]
    #print(len(NewCartDataframe))
    #    F. Transform to data format for database query
    CartDataColumns = ['XCoord','YCoord','ZCoord','CalcID','AtomID']
    CartList = NewCartDataframe[CartDataColumns].values.tolist()
    # 7. If new data exists:
    if len(CartList) > 0:
        # A. Create string to add data
        DatabaseCartString = UpdateDatabaseQuery(CartList,CartDataColumns,'cartcoords')
        # B. SQL query
        try:
            cur.execute(DatabaseCartString)
        except mariadb.Error as e4:
            print('Error inserting data into Table cartcoords: {error}'.format(error=e4))
    cur.close()
    return len(AIMList),len(CartList)

def ExtractCalcData(MoleculeDataframe):

    import os.path

    # Lists of types of calculations and basis sets
    # General note: ccsdt is a convient short hand for CCSD(T). It is NOT CCSDT.
    CalcList = ['scf','mp2','ccsd','ccsdt']
    BasisList = ['pvdz','pvtz','pvqz','pcvdz','pcvtz','pcvqz']
    NanoBasis = ['pvqz','pcvdz','pcvtz','pcvqz']
    DataList = []
    # The goal of these tools is to be able to run them in multiple different places.
    # For the moment, the program will assume a directory structure as follows:
    #   molecules/{Compound Label}/{Calculation Label}/{Basis set Label}
    CompDir = 'molecules\\'
    # CompList is list of compounds. Extract data for each compound.  
    CompList = MoleculeDataframe['MoleLabel'].values.tolist()
    CompGroup = MoleculeDataframe['MoleGroup'].values.tolist()
    Count = 0
    for i in range(len(MoleculeDataframe)):
        # Compound subdirectory if it exists
        CompPath = CompDir + CompList[i] + '\\'
        if not(os.path.isdir(CompPath)): continue
        for calc in CalcList:
            # Calculation subdirectory if it exists
            CalcPath = CompPath + calc + '\\'
            if not(os.path.isdir(CalcPath)): continue
            for basis in BasisList:
                if (basis in NanoBasis) and not (CompGroup[i] == 'nano'): continue
                # Basis set subdirectory if it exists
                BasisPath = CalcPath + basis + '\\'
                if not(os.path.isdir(BasisPath)): continue
                # Data file if it exists
                DataFileName = BasisPath + 'prp.dat'
                if os.path.exists(DataFileName):
                    # Open data file for reading
                    DataFile = open(DataFileName,'r')
                    # Initialize values for energy. MP2, CCSD, and CCSD(T) energies
                    # are not computed in every calculation.
                    Count += 1
                    if (Count % 20) == 0: print('*',end='') 
                    MP2Energy = 0.
                    CCSDEnergy = 0.
                    CCSDTEnergy = 0.
                    # Boolean variables to control when to read Rotational constants.
                    # The rotational constants are included twice in the data file.
                    # The key word is MHz. Constants are on the next line.
                    # The first time MHz is encountered, RotConst is set to true.
                    # Then, the rotational constants are extracted from the next line,
                    # and FirstRot is set to False.
                    RotConst = False
                    FirstRot = True
                    # Read each line of the data file
                    for line in DataFile:
                        # Extract the molecule's point group. This can be important
                        # for molecules with only a few atoms, but will be less 
                        # important for larger ones.
                        if 'The full molecular point group is' in line:
                            MolePntGrp = line[-6:-3]
                        if 'The computational point group is' in line:
                            CompPntGrp = line[-6:-3]
                        # Extract the number of basis functions.
                        if 'basis functions' in line:
                            NumBasFunc = int(line[12:17])
                        # Extract the SCF energy. This value is computed for each type of 
                        # calculation.
                        if 'E(SCF)=' in line:
                            SCFEnergy = float(line.split()[1])
                        # Extract MP2 energy. This value is computed during MP2, CCSD, and 
                        # CCSD(T) calculations.
                        if 'Total MP2 energy' in line:
                            MP2Energy = float(line.split()[4])
                        # Extract CCSD energy. This value is computed during CCSD and CCSD(T)
                        # calculations. It is found in two different forms.
                        if 'CCSD energy  ' in line:
                            if 'Total' in line:
                                CCSDEnergy = float(line.split()[4])
                            else:
                                CCSDEnergy = float(line.split()[2])
                        # Extract CCSD(T) energy.
                        if 'CCSD(T) energy  ' in line:
                            CCSDTEnergy = float(line.split()[2])
                        # Extract Rotational Constants
                        if RotConst:
                            # As noted above, the values of interest are in the
                            # line after MHz. Split the line string and transform
                            # the string to floating point variables.
                            RotConst = False
                            LineList = line.split()
                            RotConsts = [float(Const) for Const in LineList]
                            # There are either two or three rotational constants.
                            # Linear molecules such as H2, CO2, and C2H2 have two
                            # constants. For linear molecules, set RotConstZ to 0.
                            # All other molecules have three.
                            RotConstX = RotConsts[0]
                            RotConstY = RotConsts[1]
                            if len(RotConsts) > 2:
                                RotConstZ = RotConsts[2]
                            else:
                                RotConstZ = 0
                        # Flag line for Rotationl constants if MHz is in line
                        # The rotational constants are on the next line.
                        # Constants appear twice in output file only read first
                        # occurence.
                        if 'MHz' in line and FirstRot:
                            RotConst = True
                            FirstRot = False
                        # Extract the time required for the calculation. This
                        # time will be used to group molecules
                        if 'This computation required' in line:
                            CalcTime = float(line.split()[3])
                    # Create a unique label for each calculation
                    label = '{COMP}:{CALC}/{BAS}'.format(COMP=CompList[i],CALC=calc,BAS=basis)
                    # Combine the values and append them to DataList 
                    CalcData = [label,CompList[i],calc,basis,MolePntGrp,CompPntGrp,NumBasFunc,SCFEnergy,
                                MP2Energy,CCSDEnergy,CCSDTEnergy,RotConstX,RotConstY,RotConstZ,
                                CalcTime]
                    DataList.append(CalcData)
    #print(DataList)
    #print('Data extracted from {} files'.format(len(DataList)))
    return DataList

def ExtractCartCoordData(DataUser,MoleculeDataframe):

    import os.path
    import mariadb

    cur = DataUser.Connection.cursor()
    #print('Extracting Cartesian coordinate data')
    # Lists of types of calculations and basis sets
    # General note: ccsdt is a convient short hand for CCSD(T). It is NOT CCSDT.
    CalcList = ['scf','mp2','ccsd','ccsdt']
    BasisList = ['pvdz','pvtz','pvqz','pcvdz','pcvtz','pcvqz']
    NanoBasis = ['pvqz','pcvdz','pcvtz','pcvqz']
    AtomList = []
    CartList = []
    # Pull data from elements table into dictionary to map Atomic Symbols in coordinate
    # file to Atomic Number
    try:
        cur.execute("""SELECT UPPER(AtomicSymbol), AtomicNumber FROM elements""")
        AtomDict = dict(cur.fetchall())
    except mariadb.Error as e:
        print('Error selecting data from Table element: {error}'.format(error=e))

    # The goal of these tools is to be able to run them in multiple different places.
    # For the moment, the program will assume a directory structure as follows:
    #   molecules/{Compound Label}/{Calculation Label}/{Basis set Label}
    CompDir = 'molecules\\'
    # CompList is list of compounds. Extract data for each compound.  
    CompList = MoleculeDataframe['MoleLabel'].values.tolist()
    CompGroup = MoleculeDataframe['MoleGroup'].values.tolist()
    Count = 0
    for j in range(len(MoleculeDataframe)):
        # Compound subdirectory if it exists
        CompPath = CompDir + CompList[j] + '\\'
        if not(os.path.isdir(CompPath)): continue
        for calc in CalcList:
            # Calculation subdirectory if it exists
            CalcPath = CompPath + calc + '\\'
            if not(os.path.isdir(CalcPath)): continue
            for basis in BasisList:
                if (basis in NanoBasis) and not (CompGroup[j] == 'nano'): continue
                # Basis set subdirectory if it exists
                BasisPath = CalcPath + basis + '\\'
                if not(os.path.isdir(BasisPath)): continue
                # Data files if it exists. Data will be pulled from JMOLplot.
                # ZMATnew is to check if the geometry optimizition is complete.
                DataFileName1 = BasisPath + 'prp.dat'
                DataFileName2 = BasisPath + 'JMOLplot'
                if (os.path.exists(DataFileName1) and os.path.exists(DataFileName2)):
                    labelC = '{COMP}:{CALC}/{BAS}'.format(COMP=CompList[j],CALC=calc,BAS=basis)
                    i = 0
                    DataFile = open(DataFileName2,'r')
                    Count += 1
                    if (Count % 20) == 0: print('*',end='') 
                    for line in DataFile:
                        # Each line has one of three types of data:
                        #       1. Count of the number of atoms (line 1)
                        #       2. Blank (line 2)
                        #       3. Coordinate Data: Atomic Symbol, X, Y, Z Coord.
                        # To identify the coordinate data, divide line into parts
                        # and remove 'white space'. The length of the list with
                        # data will be for. Convert character string data to
                        # numeric data and add it to list.
                        lineList = line.split()
                        if (len(lineList) == 4) and (lineList[0] != 'X'):
                            i += 1
                            labelA = '{}_{}'.format(CompList[j],i)
                            if (calc == 'scf') and (basis == 'pvdz'):
                                AtomList.append([labelA,CompList[j],AtomDict[lineList[0]],i])
                            labelCC = labelC + '_{}'.format(i)
                            CartList.append([labelC,labelA,labelCC,AtomDict[lineList[0]],
                                             float(lineList[1]),float(lineList[2]),float(lineList[3])])
                    DataFile.close()

    #print(AtomList)
    #print('Data extracted for {} atoms and {} coordinates'.format(len(AtomList),len(CartList)))
    cur.close()

    return AtomList, CartList

def UpdateDatabase(DataUser):

    import mariadb
    import math
    import pandas as pd
    import numpy as np

    cur = DataUser.Connection.cursor()

    # Preliminaries:
    # 1. Data file for molecules table to be read into dataframe
    RefFileName = 'MoleculeTableB.xlsx'
    #print('Default file for data: {}'.format(RefFileName)) 
    MoleculeDataframe = pd.read_excel(RefFileName,index_col=0)
    TotalMoleCount = len(MoleculeDataframe)

    # Insert new molecule information into molecule table
    #print('Updating data into TABLE molecules')

    # 1. Querry molecule to get current molecules. Each MoleLabel is unique.
    cur.execute('USE moleculardata')
    cur.execute('SELECT MoleLabel FROM molecules')
    CurrentMoleculeQuery = cur.fetchall()

    # 2. Convert query to list of current molecules by flattening two dimensional
    #    query list [('label #1',),('label #2',), ...] to one dimensional list
    #    ['label #1', 'label #2', ... ]
    if len(CurrentMoleculeQuery) > 0: 
        CurrentMoleculeList = [j for sub in CurrentMoleculeQuery for j in sub]
    else:
        CurrentMoleculeList = []

    # 3. Remove any molecules currently in the database from Dataframe to be
    #    used to insert new molecules (to avoid data duplication).
    NewMoleculeDataframe = MoleculeDataframe[~MoleculeDataframe['MoleLabel'].isin(CurrentMoleculeList)]
    NewMoleCount = len(NewMoleculeDataframe)

    # 4. Clean Dataframe by adding default PubChemCID (0) and other book keeping
    valuesDic = {'PubChemCID': 0, 'JMOL': 'N'}
    typeDic = {'PubChemCID' : int}
    NewMoleculeDataframe = NewMoleculeDataframe.fillna(value=valuesDic)
    NewMoleculeDataframe = NewMoleculeDataframe.astype(typeDic)

    # 5. Convert Dataframe to a list
    MoleculeList = NewMoleculeDataframe.values.tolist()
    # Create string to add data
    if len(MoleculeList) > 0:
        MoleculeColumns = ['PubChemCID', 'MoleGroup', 'MoleLabel', 'MoleFormula',
                           'MoleName', 'SMILES', 'JMOL', 'LCSS', 'Flammable', 
                           'AcuteToxicity', 'Corrosive', 'Irritant', 'Cyclic']
        # Generate string to insert the data through a SQL query
        DatabaseMolString = UpdateDatabaseQuery(MoleculeList,MoleculeColumns,'molecules')
        # SQL query
        try:
            cur.execute(DatabaseMolString)
        except mariadb.Error as e1:
            print('Error inserting Database Table molecules: {error}'.format(error=e1))

    cur.close()

    print('Extracting Data:',end = '')
    CalcList = ExtractCalcData(MoleculeDataframe)
    AtomList, CoordList = ExtractCartCoordData(DataUser,MoleculeDataframe)
    print('\n')

    NewCalcCount = UpdateCalcData(DataUser,MoleculeDataframe,CalcList)
    NewAtomCount, NewCoordCount = UpdateCoordData(DataUser,MoleculeDataframe,AtomList,CoordList)

    TotalCalcCount = len(CalcList)
    TotalAtomCount = len(AtomList)
    TotalCoordCount = len(CoordList)

    TableList = ['molecules','calculations','atoms.','cartcoords']
    TotalCountList = [TotalMoleCount,TotalCalcCount,TotalAtomCount,TotalCoordCount]
    NewCountList = [NewMoleCount,NewCalcCount,NewAtomCount,NewCoordCount]
    SummaryString =  '+{:-<74}+\n'.format('')
    SummaryString += '|{:^74}|\n'.format('TABLE UPDATE SUMMARY')
    SummaryString += '+{:-<74}+\n'.format('')
    SummaryString += '|{:<10}|{:^15}|{:^15}|{:^15}|{:^15}|\n'.format('',*TableList)
    SummaryString += '+{:-<10}+{:-<15}+{:-<15}+{:-<15}+{:-<15}+\n'.format('','','','','')
    SummaryString += '|{:^10}|{:^15}|{:^15}|{:^15}|{:^15}|\n'.format('Extract',*TotalCountList)
    SummaryString += '|{:^10}|{:^15}|{:^15}|{:^15}|{:^15}|\n'.format('New',*NewCountList)
    SummaryString += '+{:-<10}+{:-<15}+{:-<15}+{:-<15}+{:-<15}+\n'.format('','','','','')
    print(SummaryString)
    cur.close()

def UpdateMenu(DataUser):

    import MenuTools

    UDMName = 'UPDATE DATABASE MENU'
    UDMOptions = [['A','Insert Data'],['B','Create Database'],
                  ['Q','Return to Main Menu']]
    UDMString = MenuTools.CreateMenu(UDMName,UDMOptions)

    # User prompt string connected to Main Menu
    PromptString = 'Please select an option from Database Menu: '

    # Control code for Main Menu
    Option = ''
    while (Option != 'Q') and (Option != 'q'):
        # Output menu and prompt
        print(UDMString)
        Option = input(PromptString)
        # Option A: Update Database
        if Option == 'A':
            UpdateDatabase(DataUser)
        # Option B: Create Database
        elif Option == 'B':
            CreateDatabase(DataUser)
        # Option Q: Exit Database
        elif (Option == 'Q') or (Option == 'q'):
            print('Returning to Main Menu')
        # if the user does not select a valid option, a warning is
        # printed and the menu and command prompt is reprinted.
        else:
            print('Input not recognized. Returning to Main Menu.')
            break
