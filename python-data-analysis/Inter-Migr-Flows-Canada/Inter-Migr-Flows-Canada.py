#!/opt/conda/bin/python
# Danilo Nunes Melo
# Creation date 01/21/2021
# Update date 01/21/2021
# Spark Installation Automated
# Version 1.0

# Reports
# International Migration Flows to and from Selected Countries
# Generate Graphic with HTML for Exploratory Data Analysis.


from datetime import datetime
import pandas as pd
import numpy as np
import getpass
import hashlib
import time
import os

# Creating Audit Table

FILE_NAME = 'Canada.xlsx'

#  md5sum Canada.xlsx
with open(FILE_NAME, "rb") as f:
    file_hash_md5 = hashlib.md5()
    while chunk := f.read(8192):
        file_hash_md5.update(chunk)
print(file_hash_md5.hexdigest())

# sha256sum Canada.xlsx

with open(FILE_NAME, "rb") as f:
    file_hash_256 = hashlib.sha256()
    while chunk := f.read(8192):
        file_hash_256.update(chunk)
print(file_hash_256.hexdigest())

df_canada = pd.read_excel(FILE_NAME, sheet_name='Regions by Citizenship', skiprows=20)
df_canada_citizen = pd.read_excel(FILE_NAME, sheet_name='Canada by Citizenship', skiprows=1)

LOAD_DATE = time.strftime('%Y %b %d (%a) %H:%M:%S', time.gmtime())
USER_NAME = getpass.getuser();
PATH = os.getcwd();
FILE_DATE = time.strftime('%Y %b %d (%a) %H:%M:%S', time.gmtime(os.path.getmtime(FILE_NAME)))
SIZE = os.path.getsize(FILE_NAME)
HASH_MD5 = file_hash_md5.hexdigest()
HASH_SHA256 = file_hash_256.hexdigest()

df_file_audit_trail = pd.DataFrame({'LOAD_DATE': LOAD_DATE,
                                   'FILE_NAME': FILE_NAME,
                                   'USER_NAME': USER_NAME,
                                   'PATH': PATH,
                                   'FILE_DATE': FILE_DATE,
                                   'SIZE': SIZE,
                                   'HASH_MD5': HASH_MD5,
                                   'HASH_SHA256': HASH_SHA256}, index=[0])

## Unpivot

df_canada_mlt = pd.melt(df_canada, id_vars=['Type','Coverage','AreaName','RegName'])

df_canada_citizen_mlt = pd.melt(df_canada_citizen, \
                                id_vars=['Type','Coverage','OdName','AREA','AreaName','REG','RegName','DEV','DevName'])

## Cleaning
print("### Cleaning ###")
## Canada by Citizenship
df_canada_citizen_mlt.columns = ['Type','Coverage','OdName','AREA','AreaName','REG','RegName','DEV','DevName','DateYear','PeopleQtd']
df_canada_citizen_mlt['PeopleQtd'] = pd.to_numeric(df_canada_citizen_mlt['PeopleQtd'], errors='coerce', downcast='integer')
df_canada_citizen_mlt['DateYear'] = df_canada_citizen_mlt['DateYear'].astype('int64')
df_canada_citizen_mlt['PeopleQtd'] = df_canada_citizen_mlt['PeopleQtd'].replace(np.nan, 0, regex=True).astype('int64')

## Regions by Citizenship

df_canada_mlt.columns = ['Type','Coverage','AreaName','RegName','DateYear','PeopleQtd']
df_canada_mlt['PeopleQtd'] = pd.to_numeric(df_canada_mlt['PeopleQtd'], errors='coerce', downcast='integer')
df_canada_mlt['DateYear'] = df_canada_mlt['DateYear'].astype('int64')
df_canada_mlt['PeopleQtd'] = df_canada_mlt['PeopleQtd'].replace(np.nan, 0, regex=True).astype('int64')

df_canada_migration_total = df_canada_mlt[df_canada_mlt['RegName'].isna()]

df_canada_mlt = df_canada_mlt[df_canada_mlt['RegName'].notna()]

df_canada_migration_flow = df_canada_mlt
df_canada_migration_flow[["Type","Coverage","AreaName","RegName"]] = df_canada_mlt[["Type","Coverage","AreaName","RegName"]].apply(lambda x: x.astype('category'))

df_canada_migration_citizen = df_canada_citizen_mlt
df_canada_migration_citizen[['Type','Coverage','OdName','AREA','AreaName','REG','RegName','DEV','DevName']] = df_canada_citizen_mlt[['Type','Coverage','OdName','AREA','AreaName','REG','RegName','DEV','DevName']].apply(lambda x: x.astype('category'))

## Plots
print("### Generating plot ###")

from matplotlib import pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Bubble Plot

image1="Fig-BubbleYears.jpg"

sns.set_style("darkgrid")
f, axes = plt.subplots(1,1, figsize=(11,8))

i01 = sns.scatterplot(data=df_canada_migration_flow, x="DateYear",y="PeopleQtd", hue="PeopleQtd",\
                 size="PeopleQtd", sizes=(20, 400))#, ax=axes[0,1])

plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.tight_layout()

plt.title('Shows the scatter years and quantity of people')

plt.savefig(image1)

## Areas-years vs quantity of people

image2="Fig-YearArea.png"
sns.set_style("darkgrid")
f, axes = plt.subplots(1,1, figsize=(11,8))

sca = sns.scatterplot(data=df_canada_migration_flow, x="DateYear", y="PeopleQtd", hue="AreaName", \
                 size="PeopleQtd", sizes=(20, 400))

plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.tight_layout()

plt.title('Areas-years vs quantity of people')

plt.savefig(image2)


## Linear Plot

image3="Fig-YearRegion.png"

sns.set_style("darkgrid")
f, axes = plt.subplots(1,1, figsize=(11,8))

XRegName = sns.lineplot(data=df_canada_migration_flow, x="DateYear", y="PeopleQtd", hue="RegName", \
                   markers=True, dashes=False)

plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.tight_layout()

plt.title('Distribution of the number of people by year and Region')

plt.savefig(image3)

## Line Areas-years vs quantity of people

image4="Fig-LineYearArea.png"
sns.set_style("darkgrid")
f, axes = plt.subplots(1,1, figsize=(12,7))

XAreaName = sns.lineplot(data=df_canada_migration_flow, x="DateYear", y="PeopleQtd", hue="AreaName", \
                   markers=True, dashes=False)

plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.tight_layout()

plt.title('Line Areas-years vs quantity of people')

plt.savefig(image4)

## Geo Map - Density Plot - Heatmap 

import geopandas as gpd

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
world = world[(world.name!="Antarctica")]
world_canada = pd.merge(world,
                        df_canada_migration_citizen[df_canada_migration_citizen["DateYear"]==2013], 
                        how="outer", # "left"  "right"
                        left_on=["name"], 
                        right_on=["OdName"])
world_canada['PeopleQtd'] = world_canada['PeopleQtd'].fillna(0)

image5="Map2013.png"

fig, ax = plt.subplots(1, 1, figsize=(19,10))

world_canada.plot(column='PeopleQtd',
                  ax=ax, 
                  edgecolor="black",
                  cmap='Blues', scheme='quantiles',
                  legend=True, 
                 )

plt.title('Map QTD of People 2013')

plt.savefig(image5)

## Heatmap

### Total by Area

image6="HeatmapArea.png"
df_canada_migration_total_heat = df_canada_migration_total.pivot("AreaName","DateYear","PeopleQtd")
ax = sns.heatmap(df_canada_migration_total_heat,center=df_canada_migration_total_heat.loc["Asia Total", 2000])

plt.title('Heatmap Total by Area')

plt.savefig(image6)

###Total by Region

image7="HeatmapRegion.png"

ax = plt.subplots(figsize=(8,8))

df_canada_migration_heat = df_canada_migration_flow[df_canada_migration_flow["Coverage"] == "Foreigners"] \
                                                                        .pivot("RegName","DateYear","PeopleQtd")
ax = sns.heatmap(df_canada_migration_heat,center=df_canada_migration_heat.loc["Eastern Asia", 2000])

plt.title('Heatmap Total by Region')

plt.savefig(image7)

## Generating HTML Report
print("### Generating HTML Report ###")

df_audit_html = df_file_audit_trail\
                        .to_html()\
                        .replace('<table border="1" class="dataframe">','<table class="table table-striped">')
df_audit_html

html_body = '''
<html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <style>body{ margin:0 100; background:whitesmoke; }</style>
    </head>
    <body>
        <h1>International Migration Flows to and from Selected Countries</h1>

        <!-- ### Section 1 ### --->
        <h2>Graphic 1: Shows the scatter years and quantity of people</h2>
        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image1 + '''"></iframe>

        <!-- ### Section 2 ### --->
        <h2>Graphic 2: Areas-years vs quantity of people</h2>
        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image2 + '''"></iframe>

        <!-- ### Section 3 ### --->
        <h2>Graphic 3: Distribution of the number of people by year and Region</h2>
        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image3 + '''"></iframe>


        <!-- ### Section 4 ### --->
        <h2>Graphic 4: Line Areas-years vs quantity of people</h2>
        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image4 + '''"></iframe>


        <!-- ### Section 5 ### --->
        <h2>Graphic 5: Map QTD of People 2013</h2>
        <iframe width="1600" height="550" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image5 + '''"></iframe>

        <!-- ### Section 6 ### --->
        <h2>Graphic 6: Heatmap Total by Area</h2>
        <iframe width="1000" height="300" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image6 + '''"></iframe>

        <!-- ### Section 7 ### --->
        <h2>Graphic 7: Heatmap Total by Region</h2>
        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" \
src="''' + image7 + '''"></iframe>

        <p>International Migration Flows to and from Selected Countries: The 2015 Revision. \
 International Migration Flows to and from Selected Countries: The 2015 Revision. \
(United Nations database, POP/DB/MIG/Flow/Rev.2015).</p>
        <h3>                                   </h3>
        <h3>Reference Source table: Excel File </h3>
        ''' + df_audit_html + '''
    </body>
</html>'''

file = open('./Report-Canada.html','w')
file.write(html_body)
file.close()

print("### Report Generated ###")
