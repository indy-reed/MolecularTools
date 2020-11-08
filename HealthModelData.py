def RiskTable1(DataUser):

    import pandas as pd
    import mariadb

    Connect = DataUser.OpenConnection()
    cur = Connect.cursor()

    # General notes about the data.
    #   1. There are just to enough under weight people for statistical analysis (less
    #      than 2%).
    #   2. Including heavy drinkers may not also be practical since about 6% of 
    #      those surveyed could be classed as heavy drinkers.
    #

    # Table breakdown:
    #   The goal of this table is to evaulate the risk of a patient becoming overweight.
    #   if they are normal weight. The factors to be accounted for are physical activity,
    #   smoking, age and sex.
    #   Male and female patients will be tabulated in seperate tables.
    # In principle, a person can change their physical activity and stop smoking. For
    # BMI, the data only allows for forward propogation. I can determine the risk of a
    # normal person becoming overweight, but not the chance that an overweight person
    # acheives a normal body weight.

    # 1. Male table
    #   A. Normal weight
    cur.execute('USE healthdata')
    cur.execute("""SELECT CONCAT(PACAT, '-', SMOKER) AS RISK_FACT, 
                        AGEG5YR, COUNT(*) 
                        FROM brfssdata
                        WHERE BMI5CAT = '2' AND SEX = '1'
                        GROUP BY PACAT, SMOKER, AGEG5YR""")
    CountList = cur.fetchall()
    CountColumns = ['RISK_FACT','AGEGRP','Count']
    CountDF = pd.DataFrame(CountList,columns=CountColumns)
    
    CountPTA = pd.pivot(CountDF,index='RISK_FACT',columns='AGEGRP')
    CountPTA.fillna(0,inplace=True)
    CountPTA = CountPTA.astype(int)

    #   B. Over weight
    cur.execute("""SELECT CONCAT(PACAT, '-', SMOKER) AS RISK_FACT, 
                        AGEG5YR, COUNT(*) 
                        FROM brfssdata
                        WHERE BMI5CAT = '3' AND SEX = '1'
                        GROUP BY PACAT, SMOKER, AGEG5YR""")
    CountList = cur.fetchall()
    CountColumns = ['RISK_FACT','AGEGRP','Count']
    CountDF = pd.DataFrame(CountList,columns=CountColumns)
    
    CountPTB = pd.pivot(CountDF,index='RISK_FACT',columns='AGEGRP')
    CountPTB.fillna(0,inplace=True)
    CountPTB = CountPTB.astype(int)

    #   C. Obese
    cur.execute("""SELECT CONCAT(PACAT, '-', SMOKER) AS RISK_FACT, 
                        AGEG5YR, COUNT(*) 
                        FROM brfssdata
                        WHERE BMI5CAT = '4' AND SEX = '1'
                        GROUP BY PACAT, SMOKER, AGEG5YR""")
    CountList = cur.fetchall()
    CountColumns = ['RISK_FACT','AGEGRP','Count']
    CountDF = pd.DataFrame(CountList,columns=CountColumns)
    
    CountPTC = pd.pivot(CountDF,index='RISK_FACT',columns='AGEGRP')
    CountPTC.fillna(0,inplace=True)
    CountPTC = CountPTC.astype(int)

    #   D. Reference (exclude underweight)
    cur.execute("""SELECT CONCAT(PACAT, '-', SMOKER) AS RISK_FACT, 
                        AGEG5YR, COUNT(*) 
                        FROM brfssdata
                        WHERE BMI5CAT <> '1' AND SEX = '1'
                        GROUP BY PACAT, SMOKER, AGEG5YR""")
    CountList = cur.fetchall()
    CountColumns = ['RISK_FACT','AGEGRP','Count']
    CountDF = pd.DataFrame(CountList,columns=CountColumns)
    
    CountPTR = pd.pivot(CountDF,index='RISK_FACT',columns='AGEGRP')
    CountPTR.fillna(0,inplace=True)
    CountPTR = CountPTR.astype(int)

    RatePTA = CountPTA.div(CountPTR)
    RatePTB = CountPTB.div(CountPTR)
    RatePTC = CountPTC.div(CountPTR)

    print(RatePTA)
    print(RatePTB)
    print(RatePTC)
    cur.close()
    DataUser.CloseConnection(Connect)

def ModelData(DataUser):

    import pandas as pd
    import mariadb

    RiskTable1(DataUser)