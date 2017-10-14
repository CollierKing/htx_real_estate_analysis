
# coding: utf-8

# In[1]:

from bs4 import BeautifulSoup, NavigableString, Tag
import requests
import lxml
import pandas as pd
import numpy as np
import re
import time
import os
import csv
import datetime as dt
pd.set_option('display.max_columns', 150)
pd.set_option('display.max_colwidth', -1)


# # Prod #

# ### Pull Listings in Loop

# Find max pages to loop through from initial query result

# In[2]:

cols = ['Page_Link','Page_Address','Price','Price_Change','Sys_Date']
listing_all_tot = pd.DataFrame(columns=cols)


# In[3]:

st_price_low = 90000
st_price_high = 100000


# In[4]:

while st_price_low < 550000:
    #Increase Price Ranges Sequentially
    st_price_low += 10000
    st_price_high += 10000
    #Create Query URL
    BASE_URL = "http://www.har.com/search/dosearch?sort=listprice%20asc&region_id=1&"
    #BASE_URL = BASE_URL + "for_sale=1&property_class_id=1,2,4&zip_code=77007,77008,77018,77022,77009,77020,77003,77011"
    BASE_URL = BASE_URL + "for_sale=1&property_class_id=1,2,4&zip_code=77007,77008,77018,77022,77009,77020,77003,77011,77055,77092,77018,77004"
    BASE_URL = BASE_URL + "&property_status=A&listing_price_min=" + str(st_price_low) +     "&listing_price_max=" + str(st_price_high)
    HEADERS = {'User-Agent':'Mozilla/5.0'}
    #Parse Target Page
    response = requests.get(BASE_URL,headers=HEADERS)
    soup = BeautifulSoup(response.content, "html5lib")
    #Pull Number of Pages
    pages_all = soup.findAll("li", id=lambda x: x and x.startswith('page'))
    page_list = []
    for tag in pages_all:
        page_ = tag.get('id')
        page_ = re.sub("page_","",page_)
        page_ = int(page_)
        page_list.append(page_)
    #Create empty page list
    page_links = []
    cols = ['Page_Link','Page_Address','Price','Price_Change','Sys_Date']
    listing_all = pd.DataFrame(columns=cols)
    for page in page_list:
        try:
            sub_in = "page=" + str(page) + "&sort"
            BASE_URL = re.sub("sort",sub_in,BASE_URL)
            response = requests.get(BASE_URL,headers=HEADERS)
            soup = BeautifulSoup(response.content, "html5lib")
            links = soup.findAll("a", { "class" : "address" })
            prices = soup.findAll("div", { "class" : "price" })
            page_links = []
            # Create address list
            page_addrs = []
            for link in links:
                page_links.append(link['href'])
                page_addrs.append(link.text)
            # Create price list
            price_list = []
            for price in prices:
                price_ = price.text.lstrip()
                price_ = price_.rstrip()
                price_ = re.sub("\$ ","",price_)
                price_ = re.sub(",","",price_)
                price_ = int(price_)
                price_list.append(price_)
            # Create price reduction list
            price_chg_list = []
            for price in prices:
                img = str(price.img)
                img = re.sub('<img src="http://www.har.com/resources/images/icons/',"",img)
                img = re.sub('.png"/>',"",img)
                price_chg_list.append(img)
            # Create dataframe of lists
            listing_info = pd.DataFrame(
                {'Page_Link': page_links,
                 'Page_Address': page_addrs,
                 'Price': price_list,
                 'Price_Change': price_chg_list
                })
            listing_info['Sys_Date'] = dt.datetime.today().strftime("%m/%d/%Y")
            listing_all = listing_all.append(listing_info)
            time.sleep(5)
        except:
            continue
    time.sleep(2)
    listing_all_tot = listing_all_tot.append(listing_all)
    if st_price_high % 100000 == 0:
        print(float(st_price_high/550000))


# In[5]:

len(listing_all_tot)


# In[6]:

#Remove duplicate listings
listing_all_tot = listing_all_tot.drop_duplicates(keep="first")
len(listing_all_tot)


# In[ ]:




# In[7]:

#DEV


# In[ ]:




# In[ ]:




# In[ ]:




# In[8]:

#DEV


# In[ ]:




# ### Query MySQL Database Listing Table

# In[9]:

import pymysql
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://user:password@dbname/tbname',echo=False)


# In[10]:

#Query to retrieve current listings (already appended)
saved_listings = pd.read_sql_query('SELECT * FROM har_listings_test',engine)


# In[11]:

len(saved_listings)


# ### Outer Join 1 (In Scraped Listings/Not In Saved Listings)
# This will find new listings which we just scraped but have not yet been appended to the DB table

# In[12]:

listing_new = listing_all_tot[~listing_all_tot['Page_Link'].isin(saved_listings['Page_Link'])]


# In[13]:

len(listing_new)


# In[14]:

#Append data to table
listing_new.to_sql(name="har_listings_test",con=engine,if_exists="append",index=False)


# In[15]:

listing_new = listing_new.reset_index(drop=True)


# ### Outer Join 2 (In Saved Listings/Not in Scraped Listings

# In[16]:

listing_gone = saved_listings[~saved_listings['Page_Link'].isin(listing_all_tot['Page_Link'])]


# In[17]:

len(listing_gone)


# In[18]:

listing_gone['Price_Change'] = "off_market"


# In[19]:

listings_already_gone = saved_listings[saved_listings['Price_Change']=="off_market"]


# In[20]:

len(listings_already_gone)


# In[21]:

listing_gone_new = listing_gone[~listing_gone['Page_Link'].isin(listings_already_gone['Page_Link'])]


# In[22]:

len(listing_gone_new)


# In[23]:

listing_gone_new.head()


# In[24]:

listing_gone_new['Sys_Date'] = dt.datetime.today().strftime("%m/%d/%Y")


# In[25]:

#Append data to table
listing_gone_new.to_sql(name="har_listings_test",con=engine,if_exists="append",index=False)


# ### Inner Join (In Saved Listings & In Scraped Listings

# In[26]:

# First find listings that are in both dataframes
listings_changed = listing_all_tot[listing_all_tot['Page_Link'].isin(saved_listings['Page_Link'])]


# In[27]:

len(listings_changed)


# In[28]:

# Left join on page link
listings_changed = listings_changed.merge(saved_listings,on="Page_Link",how="left")


# In[29]:

listings_changed['Price_x'] = listings_changed['Price_x'].astype(int)
listings_changed['Price_y'] = listings_changed['Price_y'].astype(int)


# In[30]:

listings_changed.head()


# In[31]:

#compare prices between records and designate as price increase/decrease
listings_changed['Price_Change_x'][(listings_changed['Price_x'] > listings_changed['Price_y'])] = 'price-increase'
listings_changed['Price_Change_x'][(listings_changed['Price_x'] < listings_changed['Price_y'])] = 'price-reduction'
listings_changed['Price_Change_x'][(listings_changed['Price_x'] == listings_changed['Price_y'])] = 'None'


# In[32]:

len(listings_changed)


# In[33]:

#subset the joined DF for the new information only
listings_changed = listings_changed[["Page_Link","Page_Address_x","Price_x","Price_Change_x","Sys_Date_x"]]


# In[34]:

#rename to join to saved listings
listings_changed.columns = ['Page_Link','Page_Address','Price','Price_Change',"Sys_Date"]


# In[35]:

#append to saved listings
saved_or_changed_listings = saved_listings.append(listings_changed)


# In[36]:

len(saved_or_changed_listings)


# In[37]:

saved_or_changed_listings = saved_or_changed_listings.sort_values(["Page_Link","Price"])


# In[38]:

saved_or_changed_listings.head(10)


# In[39]:

saved_or_changed_listings['Price'] = saved_or_changed_listings['Price'].astype(int)
saved_or_changed_listings['Page_Link'] = saved_or_changed_listings['Page_Link'].astype(str)


# In[40]:

saved_or_changed_listings.dtypes


# In[41]:

#remove duplicate link/price records
saved_or_changed_listings = saved_or_changed_listings.drop_duplicates(subset=['Page_Link','Price'],keep=False)


# In[42]:

len(saved_or_changed_listings)


# In[43]:

#subset for only today's price changes
saved_or_changed_listings = saved_or_changed_listings[saved_or_changed_listings['Sys_Date']==dt.datetime.today().strftime("%m/%d/%Y")]


# In[44]:

len(saved_or_changed_listings)


# In[45]:

#Append data to table
saved_or_changed_listings.to_sql(name="har_listings_test",con=engine,if_exists="append",index=False)


# ### Connect to MongoDB and check/append records

# In[46]:

import json
import pymongo
from pymongo import MongoClient


# In[47]:

import subprocess
subprocess.Popen('C:/Program Files/MongoDB/Server/3.4/bin/mongod.exe')


# In[48]:

client = MongoClient()
db = client.db_har_test
collection = db.collection_har_test
db.home_details_test.count()


# In[49]:

saved_details = pd.DataFrame(list(db.home_details_test.find()))


# In[50]:

listing_new_scrape = listing_new[~listing_new['Page_Link'].isin(saved_details['Page_Link'])]


# In[51]:

listing_new_scrape.reset_index(inplace=True)


# In[52]:

del listing_new_scrape['index']


# In[53]:

len(listing_new_scrape)


# ### Scrape new listings in loop

# In[54]:

# for index,i in listing_new_scrape.iterrows():
#     print(index)


# In[55]:

school_df_all = pd.DataFrame(columns=["School","Index_Rating","Distinction","Home"])
for index,i in listing_new_scrape.iterrows():
    BASE_URL = str(i["Page_Link"])
    HEADERS = {'User-Agent':'Mozilla/5.0'}
    response = requests.get(BASE_URL,headers=HEADERS)
    soup = BeautifulSoup(response.content,"html5lib")
    #Find Price
    try:
        list_price = soup.find("span", {"class":"big"}).contents[0].strip()
        list_price = re.sub("\$","",list_price)
        list_price = re.sub(",","",list_price)
        list_price = int(list_price)
    except AttributeError:
        list_price = "MISSING"
    #Find Price Reduction
    try:
        price_reduced = soup.find("span", {"class":"price_reduced"}).contents[0].strip()
        price_reduced = re.sub("Price Reduced ","",price_reduced)
        price_reduced = re.sub("% ↓","",price_reduced)
        price_reduced = float(price_reduced)
        price_reduced = price_reduced/100
    except AttributeError:
        price_reduced = 0
    #Find details
    details = soup.findAll("div",{"class":"dc_value"})
    details_list = []
    for tag in details:
        try:
            txt = tag.text.strip()
            txt = txt.lstrip()
            details_list.append(txt)
        except:
            pass
    #Find labels
    labels = soup.findAll("div",{"class":"dc_label"})
    labels_list = []
    for tag in labels:
        txt = tag.text.strip()
        txt = txt.lstrip()
        labels_list.append(txt)
    #Create details and labels DF
    dat = pd.DataFrame(
        {'Details': details_list,
         'Labels': labels_list
        })
    dat = dat.set_value(0, 'Details', list_price)
    dat = dat[["Labels","Details"]]
    dat = dat.drop_duplicates(["Labels"], keep='first')
    dat = dat.T
    dat.columns = dat.iloc[0]
    dat = dat.drop(dat.index[[0]])
    if len(dat.columns) > 1:
        dat.columns = dat.columns.str.replace('[^\w\s]','')
    dat['Price_Reduced'] = price_reduced
    #WIP (8/6/2017) --- Add school info---------------------------------------
    school_all = soup.findAll('div',{'class':'border_row'})
    for div in school_all:
        school = div.text
#         print(school)
        index_ratings = div.findAll('img',{'src': 'http://content.har.com/img/icons/star-yellow.png'})
        index_list = []
        for idx in index_ratings:
            index_ind = idx.get('title')
            index_list.append(index_ind)
        distinctions = div.findAll('img',{'src': 'http://content.har.com/img/icons/1awards.png'})
        distinction_list = []
        for distinction in distinctions:
            distinction_ind = distinction.get('title')
            distinction_list.append(distinction_ind)
        school_df = pd.DataFrame({
            'School':[str(school)],
            'Index_Rating':[str(index_list)],
            'Distinction':[str(distinction_list)],
            'Home' : i['Page_Link']
        })
        school_df_all = pd.concat([school_df_all,school_df])
    #WIP (8/6/2017) --- Add school info---------------------------------------
    #Create parent DF
    if index == 0:
        dat_all = dat
        dat_all['Page_Link'] = i["Page_Link"]
    else:
        dat['Page_Link'] = i["Page_Link"]
        #add columns missing in the parent DF
        for col in dat:
            if col not in dat_all:
                dat_all[col] = ""
        #add columns missing in the new DF
        for col in dat_all:
            if col not in dat:
                dat[col] = ""
        #sort both DF columns to match
        dat_all.sort_index(axis=1, inplace=True)
        dat.sort_index(axis=1, inplace=True)
        #append to parent DF
        dat_all = dat_all.append(dat)
    time.sleep(7)
    if index % 50 == 0:
        print(float(index/len(listing_new)))


# In[56]:

dat_all.shape


# In[80]:

# school_df_all


# In[57]:

dat_all.reset_index(drop=True,inplace=True)


# In[58]:

dat_all2 = dat_all


# In[59]:

dat_all2.columns=dat_all2.columns.str.replace('[^\w\s]','')


# In[60]:

dat_all2 = dat_all2.reset_index(drop=True)


# In[61]:

dat_all2.shape


# ### Append new records to MongoDB

# #### Details

# In[62]:

records = json.loads(dat_all2.T.to_json()).values()


# In[63]:

db.home_details_test.insert_many(records)


# In[64]:

db.home_details_test.count()


# #### Schools

# In[65]:

school_df_all['School'] = school_df_all['School'].str.replace('[^\w\s]','')


# In[66]:

school_df_all.reset_index(drop=True,inplace=True)


# In[67]:

records = json.loads(school_df_all.T.to_json()).values()


# In[68]:

db.home_details_schools3.insert_many(records)


# In[69]:

db.home_details_schools3.count()


# ### Export CSV to G-Drive

# In[70]:

dir = "C:/Users/David/Google Drive/Real_Estate"


# #### Export DF of All Listings

# In[71]:

saved_listings = pd.read_sql_query('SELECT * FROM har_listings_test',engine)


# In[72]:

os.chdir(dir)


# In[73]:

saved_listings.to_csv('HAR_listings{}.csv'.format(pd.datetime.today().strftime('%y%m%d')))


# #### Export DF of All Features

# In[74]:

saved_details = pd.DataFrame(list(db.home_details_test.find()))


# In[75]:

saved_details.to_csv('HAR_details{}.csv'.format(pd.datetime.today().strftime('%y%m%d')))


# In[76]:

school_details = pd.DataFrame(list(db.home_details_schools3.find()))


# In[77]:

school_details.to_csv('HAR_school_details{}.csv'.format(pd.datetime.today().strftime('%y%m%d')))


# In[128]:

# test = pd.read_csv("HAR_school_details170807.csv")


# In[ ]:



