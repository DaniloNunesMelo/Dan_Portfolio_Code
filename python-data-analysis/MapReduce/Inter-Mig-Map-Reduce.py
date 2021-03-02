from functools import reduce

ItalyMigFile = open("MIG_ITALY_NO_QUOTE.csv","r")
ItalyMigRead = ItalyMigFile.read()
ItalyMig = ItalyMigRead.splitlines()
ItalyMigFilter = filter(lambda rec: rec.split(",")[6] == "DEU", ItalyMig)
ItalyMigMap = map(lambda rec: float(rec.split(",")[10]),ItalyMigFilter)
ItalyMigReduce = reduce(lambda total, element: total + element, ItalyMigMap)
print(ItalyMigReduce)
# 18777088.0