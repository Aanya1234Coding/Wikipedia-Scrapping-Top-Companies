#Due by May 10th
# Project Title: Scrapping and Analyzing Top Global Companies from Wikipedia
# Project Overview:
# Scrape the table - certain techniques
# Based on Columns and store it in sqlite3 - attributes
# aNalyze the data using pandas library
#Few Questions: 
#1.> Top 5 Companies based on Revenue - industry
#2.> Top 5 Companies based on Profit - industry
#3.> Bottom 5 Companies based on Revenue - industry
#4.> Bottom 5 Companies based on Profit - industry
#5.> Top 5 Based on Employees - industry
#6.> Bottom 5 Based on Employees - industry
#7.> Average Revenue
#8.> Average Profit
#9.> Make your own questions: 

#Save the cleaned data to csv file

import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd


url = 'https://en.wikipedia.org/wiki/List_of_largest_companies_by_revenue'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

table = soup.find('table', {'class': 'wikitable'})

companies_data = []
for company in table.find_all('tr')[1:]: 
    cells = company.find_all(['td', 'th'])  
    if len(cells) >= 5: 
        name = cells[1].get_text(strip=True)
        industry = cells[2].get_text(strip=True)
        headquarters = cells[6].get_text(strip=True)
        revenue = cells[3].text.strip() 
        profit = cells[4].text.strip()
        employees = cells[5].text.strip()  
        companies_data.append((name, industry, headquarters, revenue, profit, employees))


conn = sqlite3.connect('companies.db')
cursor = conn.cursor()
cursor.execute('''
    create table if not exists companies (
        name text,
        industry text,
        headquarters text,
        revenue text,
        profit text,
        employees text
    )
''')
cursor.executemany('insert into companies (name, industry, headquarters,revenue,profit,employees) VALUES (?, ?, ?, ?, ?, ?)', companies_data)

df = pd.read_sql_query("select *from companies",conn)
df = df.drop_duplicates()

df["revenue"] = df["revenue"].str.replace(",", "").str.replace("$",'').astype(float)

df['profit'] = df['profit'].str.replace('"','').str.replace("$", "").str.replace('"','')

df['profit'] = df['profit'].str.replace(',', '').str.replace('$', '').astype(float)

df['employees'] = pd.to_numeric(df['employees'].str.replace(',', '').str.strip(), errors='coerce').dropna().astype(int)

print("Top 5 Companies Based on Revenue")
print(df.nlargest(5, "revenue")[["name", "revenue"]].to_string(index=False))
print("Top 5 Companies Based on Profit")
print(df.nlargest(5, "profit")[["name", "profit"]].to_string(index=False))
print("Bottom 5 Companies Based on Revenue")
print(df.nsmallest(5, "revenue")[["name", "revenue"]].to_string(index=False))
print("Bottom 5 Companies Based on Profit")

print(df.nsmallest(5, "profit")[["name", "profit"]].to_string(index=False))
print("Top 5 Companies Based on # of Employees")

print(df.nlargest(5, "employees")[["name", "employees"]].to_string(index=False))
print("Bottom 5 Companies Based on # of Employees")

print(df.nsmallest(5, "employees")[["name", "employees"]].to_string(index=False))
print("Average Revenue")
average_revenue = df["revenue"].mean()
print(average_revenue)
print("Average Profit")
average_profit = df["profit"].mean()
print(average_profit)

df.to_csv('c_cleaned.csv',index = False)

print("Cleaned data saved to a csv file")
conn.commit()
conn.close()


#scrap electronic appliances