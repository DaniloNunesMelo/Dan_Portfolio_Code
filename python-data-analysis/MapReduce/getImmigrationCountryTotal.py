import sys
from functools import reduce

def getImmigrationCountryTotal(path, country):
  ItalyMigFile = open(path,"r")
  ItalyMigRead = ItalyMigFile.read()
  ItalyMig = ItalyMigRead.splitlines()
  ItalyMigFilter = filter(lambda rec: rec.split(",")[7] == country, ItalyMig)
  ItalyMigMap = map(lambda rec: float(rec.split(",")[10]),ItalyMigFilter)
  try:
    ItalyMigReduce = reduce(lambda total, element: total + element, ItalyMigMap)
  except TypeError:
    print("We don't have registration for the informed Country")
  return(ItalyMigReduce)

path = sys.argv[1]
country = sys.argv[2]

print(getImmigrationCountryTotal(path, country))
# Germany 18777088.0