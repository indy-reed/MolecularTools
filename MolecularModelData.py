def ModelOptions():

    import MenuTools

    # Get the number of atoms to be included in the data set. The default is
    # 25.
    NumAtom = 25
    AAOption = 'B'
    ASOption = 'D'
    MDAADict = {'A':'Left','B':'Center','C':'Right'}
    MDASDict = {'A':'None','B':'Rotations','C':'Reflections','D':'Both'}

    DefaultPrompt = 'Do you want to use the default setting? (Y/N) '
    DefaultS = input(DefaultPrompt)
    if DefaultS == 'N' or DefaultS == 'n':
        NumAtomMin = 11
        NumAtomMax = 25
        AtomInst = 'Model requires a minimum of {} atoms and a maximum of {} atoms.'.format(NumAtomMin,
                                                                                        NumAtomMax)
        AtomErrA = 'Input not recognized. Using default maximum number of atoms: {}.'.format(NumAtom)
        print(AtomInst)
        AtomPrompt = 'Please enter the maximum number of atoms to include in data model: '
        NumAtomS = input(AtomPrompt)
        if NumAtomS.isnumeric() and int(NumAtomS) >= 11 and int(NumAtomS) <= 25:
            NumAtom = int(NumAtomS)
        else:
            print(AtomErrA)

        # Ask user how to arrange the atoms in array.
        #   Option A - Left:    fills the array in the order the atoms appeared in the input file.
        #   Option B - Center:  fills the array from the center out where the first atom goes
        #                       in the center and the other atoms are added by alternating 
        #                       between left and right.
        #   Option C - Right:   fills the array from the right side.
        #   Default option is from the center.

        MDAAName = 'MODEL MENU: Atom Arrangement'
        MDAAOptions = [['A','Left'],['B','Center'],['C','Right']]
        MDAAString = MenuTools.CreateMenu(MDAAName,MDAAOptions)
        MDAAErrA = 'Input not recognized. Using default arrangement: Center.'

        # User prompt string connected to Main Menu
        PromptString = 'Please select an option from Model Menu: '

        # Control code for Menu
        AAOption = ''
        # Output menu and prompt
        print(MDAAString)
        AAOption = input(PromptString)
        if AAOption not in ['A','B','C']:
            AAOption = 'B'
            print(MDAAErrA)

        # Ask user which additional structures to add.
        #   Option A - None:        does not add any additional structures
        #   Option B - Rotations:   adds structures where the x, y, and z coordinates
        #                           have been rearranged.
        #   Option C - Reflections: adds structures where one or more coordinates have
        #                           been multiplied by -1
        #   Option D - Both:        adds both rotations and reflections
        #   Default option is both.

        MDASName = 'MODEL MENU: Additional Structures'
        MDASOptions = [['A','None'],['B','Rotations'],['C','Reflections'],['D','Both']]
        MDASString = MenuTools.CreateMenu(MDASName,MDASOptions)
        MDASErrA = 'Input not recognized. Using default additional structures: both.'

        # User prompt string connected to Main Menu
        PromptString = 'Please select an option from Model Menu: '

        # Control code for Menu
        ASOption = ''
        # Output menu and prompt
        print(MDASString)
        ASOption = input(PromptString)
        if ASOption not in ['A','B','C','D']:
            ASOption = 'D'
            print(MDASErrA)

    ModelOptionOutput =  '\t+{:-<36}+\n'.format('')
    ModelOptionOutput += '\t| {:^15} | {:^6} | {:^7} |\n'.format('Model Setting','Option','Descr.')
    ModelOptionOutput += '\t+{:-<17}+{:-<8}+{:-<9}+\n'.format('','','')
    ModelOptionOutput += '\t| {:<15} | {:^6} | {:<7} |\n'.format('Number of Atoms',NumAtom,'')
    ModelOptionOutput += '\t| {:<15} | {:^6} | {:<7} |\n'.format('in Model','','')
    ModelOptionOutput += '\t+{:-<17}+{:-<8}+{:-<9}+\n'.format('','','')
    ModelOptionOutput += '\t| {:<15} | {:^6} | {:<7} |\n'.format('Arrangement',AAOption,MDAADict[AAOption])
    ModelOptionOutput += '\t| {:<15} | {:^6} | {:<7} |\n'.format('of Atoms','','')
    ModelOptionOutput += '\t+{:-<17}+{:-<8}+{:-<9}+\n'.format('','','')
    ModelOptionOutput += '\t| {:<15} | {:^6} | {:<7} |\n'.format('Additional',ASOption,MDASDict[ASOption])
    ModelOptionOutput += '\t| {:<15} | {:^6} | {:<7} |\n'.format('Structures','','')
    ModelOptionOutput += '\t+{:-<17}+{:-<8}+{:-<9}+\n'.format('','','')
    print(ModelOptionOutput)
    return NumAtom, AAOption, ASOption

def ReadModelData(DataUser,NumAtom,AAOption,ASOption):

    import pandas as pd
    import numpy as np

    M = NumAtom
    ModelConnection = DataUser.OpenConnection()
    ModelQuery = ModelConnection.cursor()
    ModelQuery.execute('USE moleculardata')
    ModelQuery.execute("""SELECT CONCAT(M.MoleLabel,
                ':',C.CalcType,
                '/',C.BasisSet) AS label,
        C.MoleID, AM.AtomMolNum, AM.AtomicNum, CC.XCoord, CC.YCoord, CC.ZCoord, M.Cyclic
        FROM cartcoords AS CC
        JOIN atomsinmolecules AS AM
                ON CC.AtomID = AM.AtomID
        JOIN calculations AS C
                ON C.CalcID = CC.CalcID
        JOIN molecules AS M
                ON C.MoleID = M.MoleID
        WHERE C.CalcType = 'scf' AND C.BasisSet = 'pvdz'
                AND molesize(C.MoleID) <= {}
        ORDER BY C.MoleID, AM.AtomID""".format(M))

    ModelData = ModelQuery.fetchall()
    ModelColumns = ['Label','MoleID','AtomMolNum','AtomicNumber','X','Y','Z','Cyclic']
    ModelDataframe = pd.DataFrame(ModelData,columns=ModelColumns)

    N = len(ModelDataframe['MoleID'].unique())
    DataArray = np.zeros((N,4*M))
    if AAOption == 'A':
        Location = list(range(0,4*M,4))
    elif AAOption == 'B':
        Location = list(map(lambda i: 4*((M//2)+(i*(-1)**i)//2), range(M)))
    elif AAOption == 'C':
        Location = list(reversed(range(0,4*M,4)))
    else:
        Location = list(range(0,4*M,4))
    print(Location)
    for i in range(N):
        MoleculeDataframe = ModelDataframe[['AtomMolNum','AtomicNumber','X','Y','Z']].loc[ModelDataframe['MoleID'] == i+1]
        for j in range(len(MoleculeDataframe)):
            XYZList = MoleculeDataframe[['AtomicNumber','X','Y','Z']].loc[MoleculeDataframe['AtomMolNum'] == j+1].values.tolist()
            k = Location[j]
            DataArray[i][k] = XYZList[0][0]
            DataArray[i][k+1] = XYZList[0][1]
            DataArray[i][k+2] = XYZList[0][2]
            DataArray[i][k+3] = XYZList[0][3]
    return 1

def ModelData(DataUser):

    """
        This is the entry point for constructing a data model using the
        data in moleculardata. 
        The user will be asked a series of questions to determine the
        size and scope of the data input.
        Options will include: the maximum number of atoms to included
        in the data model, how those atoms are arranged, and whether 
        to include the rotations or mirror images in the data set used
        in the model.
        The range of atoms to be allowed is 11 to 25. 
        For the test case - whether the molecule is cyclic, the smallest
        possible cyclic molecule is O3. However, to have a reasonablely
        broad data set, the minimum is set to 11 (the number of atoms in
        propane). The maximum is set to 25 (for now, which corresponds to
        100 data points in the input: atomic number, x, y, and z coordinates.
    """

    import MenuTools

    NumAtom, AAOption, ASOption = ModelOptions()
    
    Data = ReadModelData(DataUser,NumAtom,AAOption,ASOption)
