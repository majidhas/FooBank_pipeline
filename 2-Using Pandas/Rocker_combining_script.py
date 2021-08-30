from numpy.core.numeric import NaN
import pandas as pd
import re
from datetime import date


loans_urls = ["http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2017-10.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2017-11.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2017-12.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-1.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-10.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-11.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-12.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-2.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-3.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-4.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-5.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-6.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-7.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-8.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-9.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-1.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-10.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-2.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-3.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-4.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-5.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-6.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-7.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-8.csv",
"http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-9.csv"]

visits_url = "http://rocker-data-engineering-task.storage.googleapis.com/data/visits.csv"

customers_url = "http://rocker-data-engineering-task.storage.googleapis.com/data/customers.json"


visits_df = pd.read_csv(visits_url)
customers_df = pd.read_json(customers_url,lines=True)

# concatenating loan csv files in one Dataframe
loans_df = pd.read_csv(loans_urls[0],index_col=False)
for url in loans_urls[1:]:
    temp_df = pd.read_csv(url)
    loans_df = pd.concat([loans_df,temp_df],ignore_index=True)
    

#cleaning the webvisit_id column from parenthesis and and comma, where it exists
for row_number in range(len(loans_df)):
    element = loans_df.webvisit_id.iloc[row_number]
    if type(element) == str:
        loans_df.webvisit_id.iloc[row_number] = int(re.sub("[^0-9]", "", element))

# removing unused first column
del loans_df['Unnamed: 0']




# concatenating referrer and campaign field for each customer and then aggregating an joining "visits" and "loans" tables
cmpgn_ref_series = pd.Series("" for _ in range(len(loans_df)))
for i,element in enumerate(loans_df.webvisit_id):

    temp_str = ""
    if type(element) == int:
        temp_df2 = visits_df[visits_df.id == element].reset_index()     
        for index in range(len(temp_df2)):
            temp_str += temp_df2.campaign_name.iloc[index] + ',' + temp_df2.referrer.iloc[index]+"-"
        
    cmpgn_ref_series.iloc[i] = temp_str


loans_visits_df = loans_df.assign(campaign_referrer = cmpgn_ref_series.values)

#joining with customers table
merged_df = pd.merge(loans_visits_df,customers_df, how="left", left_on="user_id", right_on="id", left_index=False, right_index=False)

#removing excess column
del merged_df['id_y']

today_date = str(date.today())
merged_df.to_csv(f'Merged_loans_visits_customers_{today_date}.csv')





































