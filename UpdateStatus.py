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


