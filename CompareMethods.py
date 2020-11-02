def GroupOutput(DataTable,DataInfo,DataGroup):

    # Note DataCategory is a dictionary that contains information
    # used in the heading of the output.
    # For the atomization energy, the category is 'difference'.
    # For rotational constants, the category is 'relative difference'.
    # The title should contain the name and unit for the table being
    # summarized.

    # Compute the mean and standard deviation of the realative difference for
    # each calculation/ basis set combination
    MeanData = DataTable.mean(axis=0).values.tolist()
    StDvData = DataTable.std(axis=0).values.tolist()
    SkewData = DataTable.skew(axis=0).values.tolist()

    # Print summary table with the mean, standard deviation (Std Dv) and confidence interval (95%)
    # The confidence interval is 1.96 times the standard of deviation.

    CI95 = 1.96
    TopLine = 'Comparison of ' + DataInfo['Category'] + ' of'
    ComparisonH = '\t+{:-<59}+\n\t|{:^59}|\n\t|{:>17} {:<41}|\n'.format('',                                       
    TopLine,len(DataTable),DataInfo['Title'])
    ComparisonA = '\t+{:-<20}+{:-<12}+{:-<12}+{:-<12}+\n'.format('','','','')
    ComparisonB = '\t|{:^20}|{:^12}|{:^12}|{:^12}|\n'.format('Method/Basis',
                                                         'Mean','Std Dv','95% CI')
    ComparisonD = ''
    GpOut = DataGroup.PrintComboList()
    for i in range(len(GpOut)):
        ComparisonD += '\t|{:^20}|{:^12.6f}|{:^12.6f}|{:^12.6f}|\n'.format(GpOut[i],
                                          MeanData[i],StDvData[i],CI95*StDvData[i])
    Comparison = ComparisonH + ComparisonA + ComparisonB + ComparisonA + ComparisonD + ComparisonA
    print (Comparison)


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
    CompareConnection = DataUser.OpenConnection()
    CompareQuery = CompareConnection.cursor()
    CompareQuery.execute('USE moleculardata')
    CompareQuery.execute("""SELECT M.MoleLabel, CONCAT(C.CalcType, '/' ,C.BasisSet) AS CB, 
                                C.RotConst{}
                                FROM calculations AS C
                                JOIN molecules AS M
                                    ON C.MoleID = M.MoleID""".format(CoordLabel))
    BData = CompareQuery.fetchall()
    CompareQuery.close()
    DataUser.CloseConnection(CompareConnection)

    # Transform data to dataframe
    BColumns = ['MoleLabel','calc/basis','B']
    if len(BData) == 0: 
        return pd.DataFrame(columns=BColumns)
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

def CompareRotationalConstants(DataUser,CompareGroup):

    import pandas as pd

    # Obtain user option which corresponds to the index in Groups 
    BxyzPivotTable = pd.DataFrame()
    for Coord in ['X','Y','Z']:
        BPivotTable = GetRotationalData(DataUser,CompareGroup,Coord)
        BxyzPivotTable = BxyzPivotTable.append(BPivotTable)

    if len(BxyzPivotTable) > 0:
        # Compute the relative difference of the rotational constant with respect to
        # the reference for the data set.
        #
        #    (Bx - Bx,r) / Bx,r
        BxyzPivotRef = BxyzPivotTable[CompareGroup.RefComboTuple('B')]
        BxyzPivotTable = BxyzPivotTable.sub(BxyzPivotRef,axis=0)
        BxyzPivotTable = BxyzPivotTable.div(BxyzPivotRef,axis=0)

        # Print summary table with the mean, standard deviation (Std Dv) and confidence interval (95%)
        # The confidence interval is 1.96 times the standard of deviation.
        BxyzDataInfo = {'Category':'relative differences','Title':'Rotational Constants (MHz)'}
        GroupOutput(BxyzPivotTable, BxyzDataInfo, CompareGroup)

def GetEnergyData(DataUser,CompGroup):

    import pandas as pd

    # Query the data base for rotational constants
    CompareConnection = DataUser.OpenConnection()
    CompareQuery = CompareConnection.cursor()
    # The query will be divided into four section based on the method used to calculate the
    # energy. Each of the sections will be combine with a union and returned as one query
    # result. 
    CompareQuery.execute('USE moleculardata')
    CompareQuery.execute("""(SELECT M.MoleLabel, CONCAT(C.CalcType, '/' ,C.BasisSet) AS CB, 
                                C.SCFEnergy AS Energy
                                FROM calculations AS C
                                JOIN molecules AS M
                                    ON C.MoleID = M.MoleID
                                WHERE C.CalcType = 'scf')
                            UNION
                            (SELECT M.MoleLabel, CONCAT(C.CalcType, '/' ,C.BasisSet) AS CB, 
                                C.MP2Energy AS Energy
                                FROM calculations AS C
                                JOIN molecules AS M
                                    ON C.MoleID = M.MoleID
                                WHERE C.CalcType = 'mp2')
                            UNION
                            (SELECT M.MoleLabel, CONCAT(C.CalcType, '/' ,C.BasisSet) AS CB, 
                                C.CCSDEnergy AS Energy
                                FROM calculations AS C
                                JOIN molecules AS M
                                    ON C.MoleID = M.MoleID
                                WHERE C.CalcType = 'ccsd')
                            UNION
                            (SELECT M.MoleLabel, CONCAT(C.CalcType, '/' ,C.BasisSet) AS CB, 
                                C.CCSDTEnergy AS Energy
                                FROM calculations AS C
                                JOIN molecules AS M
                                    ON C.MoleID = M.MoleID
                                WHERE C.CalcType = 'ccsdt')""")
    EData = CompareQuery.fetchall()
    CompareQuery.close()
    DataUser.CloseConnection(CompareConnection)

    # Transform data to dataframe
    EColumns = ['MoleLabel','calc/basis','E']
    # If the query returns an empty list of data, return empty dataframe.
    if len(EData) == 0: 
        return pd.DataFrame(columns=EColumns)
    EDataframe = pd.DataFrame(EData,columns=EColumns)

    # Create pivot table:
    # Dataframe:
    # Label   calc/basis  E
    #   A1       B1      C1
    #   A1       B2      C2
    #   A2       B1      C3
    #   A2       B2      C4
    #    pivot table
    #           B1       B2
    #   A1      C1       C2
    #   A2      C3       C4
    EPivotTable = pd.pivot(EDataframe,index='MoleLabel',columns='calc/basis')

    # Clean pivot table by:
    # 1. Select the calculation/basis set combinations associated with the data
    #    the user wants to compare
    EPivotTable = EPivotTable[CompGroup.PivotComboList('E')]
    # 2. Remove any rows where the rotational constant is NaN
    EPivotTable.dropna(axis=0,how='any',inplace=True)

    return EPivotTable

def GetAtomizeEnergyData(DataUser,CompGroup):

    import pandas as pd

    # Query the data base for rotational constants
    CompareConnection = DataUser.OpenConnection()
    CompareQuery = CompareConnection.cursor()
    # The query will be divided into four section based on the method used to calculate the
    # energy. Each of the sections will be combine with a union and returned as one query
    # result. In this query, the idea is to group data and then sum it up. 
    CompareQuery.execute('USE moleculardata')
    CompareQuery.execute("""(SELECT M.MoleLabel, CONCAT('scf/' ,E.BasisSet) AS CB,
                                SUM(E.SCFEnergy) AS AtomizeEnergy
                            FROM atomsinmolecules AS A
                            JOIN atomenergy AS E
                                ON A.AtomicNum = E.AtomicNumber
                            JOIN molecules AS M
                                ON A.MoleID = M.MoleID
                            GROUP BY M.MoleLabel, E.BasisSet)
                            UNION
                            (SELECT M.MoleLabel, CONCAT('mp2/' ,E.BasisSet) AS CB,
                                SUM(E.MP2Energy) AS AtomizeEnergy
                            FROM atomsinmolecules AS A
                            JOIN atomenergy AS E
                                ON A.AtomicNum = E.AtomicNumber
                            JOIN molecules AS M
                                ON A.MoleID = M.MoleID
                            GROUP BY M.MoleLabel, E.BasisSet)
                            UNION
                            (SELECT M.MoleLabel, CONCAT('ccsd/' ,E.BasisSet) AS CB,
                                SUM(E.CCSDEnergy) AS AtomizeEnergy
                            FROM atomsinmolecules AS A
                            JOIN atomenergy AS E
                                ON A.AtomicNum = E.AtomicNumber
                            JOIN molecules AS M
                                ON A.MoleID = M.MoleID
                            GROUP BY M.MoleLabel, E.BasisSet)
                            UNION
                            (SELECT M.MoleLabel, CONCAT('ccsdt/' ,E.BasisSet) AS CB,
                                SUM(E.CCSDTEnergy) AS AtomizeEnergy
                            FROM atomsinmolecules AS A
                            JOIN atomenergy AS E
                                ON A.AtomicNum = E.AtomicNumber
                            JOIN molecules AS M
                                ON A.MoleID = M.MoleID
                            GROUP BY M.MoleLabel, E.BasisSet)""")
    AEData = CompareQuery.fetchall()
    CompareQuery.close()
    DataUser.CloseConnection(CompareConnection)

    # Transform data to dataframe
    AEColumns = ['MoleLabel','calc/basis','E']
    # If the query returns an empty list of data, return empty dataframe.
    if len(AEData) == 0: 
        return pd.DataFrame(columns=AEColumns)
    AEDataframe = pd.DataFrame(AEData,columns=AEColumns)

    # Create pivot table:
    # Dataframe:
    # Label   calc/basis  E
    #   A1       B1      C1
    #   A1       B2      C2
    #   A2       B1      C3
    #   A2       B2      C4
    #    pivot table
    #           B1       B2
    #   A1      C1       C2
    #   A2      C3       C4
    AEPivotTable = pd.pivot(AEDataframe,index='MoleLabel',columns='calc/basis')

    # Clean pivot table by:
    # 1. Select the calculation/basis set combinations associated with the data
    #    the user wants to compare
    AEPivotTable = AEPivotTable[CompGroup.PivotComboList('E')]
    # 2. Remove any rows where the rotational constant is NaN
    AEPivotTable.dropna(axis=0,how='any',inplace=True)

    return AEPivotTable

def CompareAtomizationEnergies(DataUser,CompareGroup):

    import DataGroup

    # Obtain pivot tables with the energy of the molecules and atomization
    # energy.
    EPivotTable = GetEnergyData(DataUser,CompareGroup)
    AEPivotTable = GetAtomizeEnergyData(DataUser,CompareGroup)
    #print(EPivotTable)
    # Select molecules in EPivotTable from AEPivotTable
    AEPivotTable = AEPivotTable[AEPivotTable.index.isin(EPivotTable.index)]
    #print(AEPivotTable)

    AETableAU = AEPivotTable.sub(EPivotTable)
    AETableRef = AETableAU[CompareGroup.RefComboTuple('E')]

    FactorHart2kcal = 627.5
    AETableAU = AETableAU.sub(AETableRef,axis=0)
    AETablekcal = AETableAU.mul(FactorHart2kcal)

    # Print summary table with the mean, standard deviation (Std Dv) and confidence interval (95%)
    # The confidence interval is 1.96 times the standard of deviation.
    AEDataInfo = {'Category':'differences','Title':'Atomization Energy (kcal/mol)'}
    GroupOutput(AETablekcal,AEDataInfo,CompareGroup)

def CompareRelativeEnergies(DataUser,CompareGroup):

    import DataGroup
    import pandas as pd

    # To compare the relative energies, I will be focusing on the difference between
    # isomers. The reference point will be isomer A and rotational conformation 1.
    # The method will be as follows: 
    # First, get a list of all the molecules with more than one stable (no imaginary 
    # harmonic frequencies and gradient norm less than 1E-4. The stability will be 
    # accounted when adding molecules to the source directory). Then, get the energy, 
    # label and method/basis combination for molecules in the list.
    # Second, transform this list into a pivot table where the rows are the molecule
    # labels, the columns are the method/basis combination and the values are the
    # energy.
    # Third, create a new table of the same size and shape with the energies of the
    # reference isomer.
    # Four, subtract the two tables (Isomer table and Reference table).
    # Five, select the reference column (method/basis combination to be used for comparison)
    # Six, subtract reference column. 
    # Seven, output average, deviation, etc.

    # Step 1: Get dataframe of molecular energies
    CompareConnection = DataUser.OpenConnection()
    CompareQuery = CompareConnection.cursor()
    CompareQuery.execute('USE moleculardata')
    CompareQuery.execute("""(SELECT M.MoleLabel, CONCAT(C.CalcType,'/',C.BasisSet) AS CB,
                C.SCFEnergy AS Energy
        FROM calculations AS C
        JOIN molecules AS M
                ON C.MoleID = M.MoleID
        WHERE C.CalcType = 'scf' AND M.MoleFormula IN
                (SELECT MoleFormula
                        FROM molecules
                        GROUP BY MoleFormula
                        HAVING COUNT(*) > 1))
        UNION
        (SELECT M.MoleLabel, CONCAT(C.CalcType,'/',C.BasisSet) AS CB,
                C.MP2Energy AS Energy
        FROM calculations AS C
        JOIN molecules AS M
                ON C.MoleID = M.MoleID
        WHERE C.CalcType = 'mp2' AND M.MoleFormula IN
                (SELECT MoleFormula
                        FROM molecules
                        GROUP BY MoleFormula
                        HAVING COUNT(*) > 1))
        UNION
        (SELECT M.MoleLabel, CONCAT(C.CalcType,'/',C.BasisSet) AS CB,
                C.CCSDEnergy AS Energy
        FROM calculations AS C
        JOIN molecules AS M
                ON C.MoleID = M.MoleID
        WHERE C.CalcType = 'ccsd' AND M.MoleFormula IN
                (SELECT MoleFormula
                        FROM molecules
                        GROUP BY MoleFormula
                        HAVING COUNT(*) > 1))
        UNION
        (SELECT M.MoleLabel, CONCAT(C.CalcType,'/',C.BasisSet) AS CB,
                C.CCSDTEnergy AS Energy
        FROM calculations AS C
        JOIN molecules AS M
                ON C.MoleID = M.MoleID
        WHERE C.CalcType = 'ccsdt' AND M.MoleFormula IN
                (SELECT MoleFormula
                        FROM molecules
                        GROUP BY MoleFormula
                        HAVING COUNT(*) > 1));""")
    IsoEData = CompareQuery.fetchall()
    CompareQuery.close()
    DataUser.CloseConnection(CompareConnection)

    # Transform data to dataframe
    EColumns = ['MoleLabel','calc/basis','E']
    # If the query returns an empty list of data, return empty dataframe.
    if len(IsoEData) == 0: 
        return pd.DataFrame(columns=EColumns)
    IsoEDataframe = pd.DataFrame(IsoEData,columns=EColumns)

    # Step 2: Create pivot table:
    # Dataframe:
    # Label   calc/basis  E
    #   A1       B1      C1
    #   A1       B2      C2
    #   A2       B1      C3
    #   A2       B2      C4
    #    pivot table
    #           B1       B2
    #   A1      C1       C2
    #   A2      C3       C4
    IsoEPivotTable = pd.pivot(IsoEDataframe,index='MoleLabel',columns='calc/basis')

    # Clean pivot table by:
    # 1. Select the calculation/basis set combinations associated with the data
    #    the user wants to compare
    IsoEPivotTable = IsoEPivotTable[CompareGroup.PivotComboList('E')]
    # 2. Remove any rows where the rotational constant is NaN
    IsoEPivotTable.dropna(axis=0,how='any',inplace=True)

    # Step 3: Create dictionary where the key (MoleLabel) corresponds to the 
    # isomer reference. Note: the basic form of MoleLabel c2h6o2A1 where the
    # molecular formula letters are lower case. The first capital letter 
    # corresponds to either the isomer (or in a few cases, a rotational 
    # conformation if only one isomer exists).
    # Step 3.A: Create a list of the Reference Isomers. For an isomer to be
    #           a reference it must either end in 'A' without any other capital
    #           letters preceeding it, or 'A1' without any other capital letters
    #           preceeding it. So, c4h6A and c4h8oA1 are identified as the 
    #           reference isomers while c4h6B, c4h6BA, c4h8oAA1, c4h8oA10 are not.   
    MoleLabList = IsoEPivotTable.index.values.tolist()
    MoleRefList = []
    for MoleLab in MoleLabList:
        if MoleLab[-1] == 'A' and not MoleLab[-2].isupper():
            MoleRefList.append(MoleLab)
        if MoleLab[-2:] == 'A1' and not MoleLab[-3].isupper():
            MoleRefList.append(MoleLab)

    # Step 3.B: Create a dictionary which connects each isomer with its reference
    #           isomer (if it was found earlier). For now, c2h4oC1 is the only available
    #           compound of c2h4o isomers. c2h4oA (or c2h4oA1) is not in the previous list
    #           and c2h4oC1 is not in the dictionary.
    #           For each label in the list, find the Mole Formula (lowercase letters and 
    #           numbers before the first uppercase letter which is used to label the isomer.
    #           Then, find the MoleRef that matches the Mole Formula matches and add it
    #           to the dictionary. Mole Label is the dictionary key.
    #           Also, count up each isomer associated with reference.
    MoleRefDict = {}
    CountList = [0 for i in range(len(MoleRefList))]
    MoleRefCount = dict(zip(MoleRefList,CountList))
    for MoleLab in MoleLabList:
        MoleFrm = ''
        for MoleChr in MoleLab:
            if MoleChr.isupper(): break
            MoleFrm += MoleChr

        for MoleRef in MoleRefList:
            if (MoleRef[-1] == 'A') and MoleRef[:-1] == MoleFrm:
                MoleRefDict.update({MoleLab:MoleRef})
                MoleRefCount[MoleRef] += 1
                break
            elif (MoleRef[-1] == '1') and MoleRef[:-2] == MoleFrm:
                MoleRefDict.update({MoleLab:MoleRef})
                MoleRefCount[MoleRef] += 1
                break

    # Step 3.C: Clean list and dictionary. Drop any molecule without data for another
    #           isomer. Drop any molecule without a reference. 
    MoleRefCleanList = []
    for MoleRef in MoleRefList:
        if MoleRef in MoleRefDict and MoleRefCount[MoleRef] > 1:
            MoleRefCleanList.append(MoleRef)

    MoleLabCleanList = []
    for MoleLab in MoleLabList:
        if MoleLab in MoleRefDict and MoleRefDict[MoleLab] in MoleRefCleanList:
            MoleLabCleanList.append(MoleLab)
    
    IsoEPivotTableClean = IsoEPivotTable[IsoEPivotTable.index.isin(MoleLabCleanList)]

    # Copy IsoEPivotTableClean to IsoRefPivotTable
    IsoRefPivotTable = pd.DataFrame()
    for MoleLabel in IsoEPivotTableClean.index:
        IsoRefPivotTable = IsoRefPivotTable.append(IsoEPivotTable[IsoEPivotTable.index == MoleLabel])

    for Combo in IsoRefPivotTable.columns:
        IsoRefPivotTable[Combo] = list(map(lambda i: IsoEPivotTableClean.at[MoleRefDict[i],Combo],IsoEPivotTableClean.index))
    
    IsoDiffTableAU = IsoEPivotTableClean.sub(IsoRefPivotTable)
    IsoTableRefAU = IsoDiffTableAU[CompareGroup.RefComboTuple('E')]

    FactorHart2kcal = 627.5
    IsoDiffTableAU = IsoDiffTableAU.sub(IsoTableRefAU,axis=0)
    IsoDiffTablekcal = IsoDiffTableAU.mul(FactorHart2kcal)
    # To avoid averaging zeroes associated with the reference isomer, exclude (~) any
    # index (MoleLabel) in MoleRefCleanList.
    IsoDiffTablekcal = IsoDiffTablekcal[~IsoDiffTablekcal.index.isin(MoleRefCleanList)]

    # Print summary table with the mean, standard deviation (Std Dv) and confidence interval (95%)
    # The confidence interval is 1.96 times the standard of deviation.
    IsoDiffDataInfo = {'Category':'differences','Title':'Isomer Energy (kcal/mol)'}
    GroupOutput(IsoDiffTablekcal,IsoDiffDataInfo,CompareGroup)

def CompareMethods(DataUser):

    import DataGroup

    Groups = ['nano','small','medium','large']
    A = GroupMenuOption(Reference='Calculation Comparison',DataSets=Groups)
    CompareGroup = DataGroup.DataGroup(A)
    CompareRotationalConstants(DataUser,CompareGroup)
    CompareAtomizationEnergies(DataUser,CompareGroup)
    CompareRelativeEnergies(DataUser,CompareGroup)
    Pause = input('Press <Enter> to return to Main Menu.')
