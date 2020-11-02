
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
