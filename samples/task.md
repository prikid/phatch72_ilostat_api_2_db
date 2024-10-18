I need someone to do a bulk data download/load function for me.
I need the ILO data- you can find the high-level information here: ILOSTAT API https://rplumber.ilo.org/__docs__/
I put in a few code guides below- assume I know nothing about coding. You may find what I wrote below is trash.
I am attaching the list of indicators (the attached spreadsheet). There are many.
Each pulled CSV file should be identical in structure- i.e., the same columns and data types. Create one MySQL database, and one table in the database that has the same schema as the CSV files.
For each indicator:
Get the related CSV file. Code will look like this? I am not a developer… you may need to fix this. (One note- ILO gives options for appending query strings. No need- just pull the csv file for the indicator ID.)
import pandas
dat = pandas.read_csv("https://rplumber.ilo.org/data/indicator/?[indicator ID from spreadsheet]	
Load the downloaded CSV file into the single (common) MySQL table. 
Go on to the next indicator….
Fair warning… the total number of records inserted into the one MySQL db/table will be a lot… likely somewhere around 163 – 164 million. I think the scripting is straightforward. However, the 
Thoughts?