
def CreateMenu(Title,Options):
    MenuString =  '\t+{:-<46}+\n'.format('')
    MenuString += '\t|{:^46}|\n'.format(Title)
    MenuString += '\t+{:-<46}+\n'.format('')
    MenuString += '\t|{:^12}|{:^33}|\n'.format('Options','Data Task')
    MenuString += '\t+{:-<12}+{:-<33}+\n'.format('','')
    for Option in Options:
        MenuString += '\t|{:^12}| {:32}|\n'.format(Option[0],Option[1])
    MenuString += '\t+{:-<12}+{:-<33}+\n'.format('','')
    return MenuString
