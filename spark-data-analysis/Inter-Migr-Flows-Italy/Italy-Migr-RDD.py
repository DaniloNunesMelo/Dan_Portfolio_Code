#!/opt/conda/bin/pyspark
# Danilo Nunes Melo
# Creation date 01/24/2021
# Update date
# Generate HTML Report
# Version 1.0

# Reports
# Italian citizenship acquisition
# Generate Graphic with HTML for Exploratory Data Analysis.

from pyspark import SparkContext

## Defining Spark Context
conf = SparkConf().setAppName("ItalyMigApp").setMaster("spark://172.25.0.101:7077")
sc = SparkContext(conf=conf)

## Reading flat file
rdd_mig_italy = sc.textFile("/home/jovyan/spark/MIG_ITALY_NO_QUOTE.csv", 4, use_unicode=True).repartition(6)


## Schema RDD

fields = ('CO2','Country_Nationality','VAR','Variable','GEN','Gender','COU','Country','YEA','Year','Value')
from collections import namedtuple
Countries = namedtuple('Italy', fields )
def parseRecords( line ):
    fields = line.split(",")
    return Countries(fields[0], fields[1], fields[2], fields[3], fields[4],
                             fields[5], fields[6], fields[7], fields[8], fields[9], int(fields[10]))

df_rdd_mig_italy = rdd_mig_italy.map( lambda record: parseRecords(record) )

## Map Reduce for top 10 countries
rdd_countries = df_rdd_mig_italy.map( lambda record: (record.Country, record.Value) ).reduceByKey( lambda a, b: a+b)
rdd_countries_top10 = rdd_countries.filter( lambda record: record[1] > 1000000 )
list_countries_top10 = rdd_countries_top10.sortBy( lambda a: a[1]).collect()


## Simple plots
from matplotlib import pyplot as plt

image1= "italy-image1"
listCountries = []
listValues = []


fig1, ax1 = plt.subplots(1,1, figsize=(11,8))
explode = (0, 0, 0, 0, 0, 0, 0, 0, 0 , 0.2)

for (a,b) in list_countries_top10:
    listCountries.append(a)
    listValues.append(b)
ax1.pie(listValues, explode=explode, autopct='%1.1f%%', shadow=True, startangle=90)

plt.tight_layout()
plt.title('Shows top 10 countries for Italian citizenship acquisition')

plt.legend(labels=listCountries, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

ax1.axis('equal')
plt.savefig(image1)

print("Generating HTML")
## Generating HTML

html_body = '''
<html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <style>body{ margin:0 100; background:whitesmoke; }</style>
    </head>
    <body>
        <h1>Italian citizenship acquisition</h1>

        <!-- ### Section 1 ### --->
        <h2>Graphic 1: Shows top 10 countries for Italian citizenship acquisition</h2>
        <iframe width="1100" height="800" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image1 + '''"></iframe>
    </body>
</html>'''
file = open('./Report-Italy.html','w')
file.write(html_body)
file.close()
print("Done")
