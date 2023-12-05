import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import requests
import sqlite3
import datetime as dt

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    time = dt.datetime.now()
    time = time.strftime("%Y_%m_%d-%H:%M:%S : ")
    log = time + message + "\n"
    with open("code_log.txt", "a") as logfile:
        logfile.write(log)

def extract(url, table_attribs):
    webdata = requests.get(url)
    if webdata.status_code != 200:
        pass
        log_progress(f"Failed to retrieve data, status code: {webdata.status_code}")
        exit()
    else:
        df = pd.DataFrame(columns=table_attribs)
        soup = BeautifulSoup(webdata.content, "html.parser")
        content = soup.find(id="mw-content-text") # Finds the right "body"
        content_refined = content.find("div", class_="mw-parser-output") # the "body" within the "body", not sure why they're separate like that
        table = content_refined.find("table") # finds the first table, which is the one under the 'by market captialization' header
        table_body = table.find("tbody") # Finds the table body
        rows = table_body.find_all("tr")
        for row in rows:
            # Find the names:
            aTags = row.find_all("a")
            if not aTags: # skips any rows that don't have a tags
                continue
            name = aTags[1].text

            # Find the Market Cap:
            tds = row.find_all("td")
            marketCap = float(tds[2].contents[0][:-1])
            print(marketCap, type(marketCap))
            # marketCap = tds[2].text.strip("\n")
            # marketCap = float(marketCap) # Make the data float type
            # df = pd.concat([df, pd.DataFrame([{"Name" : name, "MC_USD_Billion":marketCap}])], ignore_index=True)
        log_progress("Data extraction complete. Initiating Transformation process.")
        return df

# def transform(df, csv_path):
#     with open(csv_path, "r") as exchangefile:
#         exchangedf = pd.read_csv(exchangefile)
#         edict = exchangedf.set_index('Currency').to_dict()['Rate'] # convert the dataframe to a dictionary

#     df['MC_EUR_Billion'] = [np.round(x*edict['EUR'],2) for x in df['MC_USD_Billion']]
#     df['MC_GBP_Billion'] = [np.round(x*edict['GBP'],2) for x in df['MC_USD_Billion']]
#     df['MC_INR_Billion'] = [np.round(x*edict['INR'],2) for x in df['MC_USD_Billion']]
#     log_progress("Data transformation complete. Initiating Loading process.")
#     return df

# def load_to_csv(df, output_path):
#     df.to_csv(output_path, index=False)
#     log_progress("Data saved to CSV file")

# def load_to_db(df, sql_connection, table_name):
#     df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
#     log_progress("Data loaded to Database as a table, Executing queries.")

# def run_query(query_statement, sql_connection):
#     cur = sql_connection.cursor()
#     results = cur.execute(f"""{query_statement}""")
#     sql_connection.commit()
#     output = results.fetchall()
#     for row in output:
#         print(row)
#     log_progress("Process Complete.")
#     return output

# Preliminary stuff:
SourceURL = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
table_name = "Largest_banks"
db_name = "Banks.db"
log_progress("Preliminaries complete. Initiating ETL process.")

# Create dataframes and 
dataframe = extract(SourceURL, table_attribs)
# dataframe = transform(dataframe, csv_path)
# load_to_csv(dataframe, "./Largest_banks_data.csv")

# dbcon = sqlite3.connect(db_name)
# log_progress("SQL Connection initiated.")
# load_to_db(dataframe, dbcon, table_name)

# run_query("SELECT * FROM Largest_banks", dbcon)
# print()
# run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", dbcon)
# print()
# run_query("SELECT Name from Largest_banks LIMIT 5", dbcon)


# dbcon.close()
# log_progress("Server Connection closed.")
# # print(dataframe)