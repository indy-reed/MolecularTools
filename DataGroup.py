
class DataGroup:

    """
        This class is intended to hold information about how calculation basis set
        combinations are grouped. In principle, this information could be in a list,
        this list is proving unwieldy and can only be used once. The class needs to 
        be able to generate lists that can be used to select specific rows from
        pivot tables (stored as data frames).
    """
    
    # Set of global tuples that will be referenced within the object.
    # Tuples are used to limit changes that can be made.
    #
    # _CalcLabel contains a simplified version of the different types of 
    # calculations that are included in the database. For most cases, it is 
    # simply the lower case version of the standard abbreviation. For CCSD(T),
    # it is the ccsdt since including () is not practical in the database.
    # Higher order calculations will not be included.
    #
    # _BasisLabel contains a simplified version of the different basis sets
    # that are included in the database. The abbreviation can be transformed 
    # to the formal term.
    _GroupName = ('nano','small','medium','large','macro')
    _CalcLabel = ('scf','mp2','ccsd','ccsdt')
    _BasisLabel = ('pvdz','pvtz','pvqz','pcvdz','pcvtz','pcvqz')

    # Object constructor
    # The group will be constructed from an index which corresponds to the list of
    # options: (0) nano, (1) small, (2) medium, (3) large and (4) macro. The default 
    # will be set at large (or 3). Note: the first element of a list in standard form
    # starts with 0. 
    def __init__(self, Index=3):

        # Check if Index is less than 0 or greater than 4. If Index is not in the range,
        # alter user that the default has been set to default GroupID (3).
        if (Index < 0 ) or (Index > 4):
            self._GroupID = 3
        else:
            self._GroupID = Index
        # Set the number of calculations and basis sets to be included based on
        # the GroupID.
        if self._GroupID == 0:
            self._CalcNum = 4
            self._BasisNum = 6
        elif self._GroupID == 1:
            self._CalcNum = 4
            self._BasisNum = 2
        elif self._GroupID == 2:
            self._CalcNum = 4
            self._BasisNum = 1
        elif self._GroupID == 3:
            self._CalcNum = 2
            self._BasisNum = 1
        elif self._GroupID == 4:
            self._CalcNum = 1
            self._BasisNum = 1

    def PrintComboList(self):

        ComboList = []
        for CalcID in range(self._CalcNum):
            Calc = self._CalcLabel[CalcID].upper()
            if Calc == 'CCSDT': Calc = 'CCSD(T)'
            for BasisID in range(self._BasisNum):
                Basis = self._BasisLabel[BasisID][1:]
                ComboList.append('{}/cc-p{}'.format(Calc,Basis))
        return ComboList

    def PivotComboList(self,PivotColumnLabel='A'):

        ComboList = []
        for CalcID in range(self._CalcNum):
            Calc = self._CalcLabel[CalcID]
            for BasisID in range(self._BasisNum):
                Basis = self._BasisLabel[BasisID]
                ComboList.append((PivotColumnLabel,'{}/{}'.format(Calc,Basis)))
        return ComboList

    def RefComboTuple(self,PivotColumnLabel='A'):

        return (PivotColumnLabel,
                '{}/{}'.format(self._CalcLabel[self._CalcNum-1],self._BasisLabel[self._BasisNum-1]))
    
    def Name(self):
        return self._GroupName[self._GroupID] 
