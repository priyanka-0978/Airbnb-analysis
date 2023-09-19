#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pymongo
import mysql.connector
import plotly.express as px
import matplotlib.pyplot as plt


# In[2]:


client= pymongo.MongoClient("mongodb+srv://priya123:FLAXnahS95SgJLHA@cluster0.uw7a71f.mongodb.net/test?authMechanism=DEFAULT")
db = client['sample_airbnb']
col=db['listingsAndReviews']


# In[3]:


db.list_collection_names()


# In[4]:


data1= []
for i in col.find({}, {'_id': 1, 'listing_url': 1, 'name': 1, 'property_type': 1, 'room_type': 1, 'bed_type': 1,
                                       'minimum_nights': 1, 'maximum_nights': 1, 'cancellation_policy': 1, 'accommodates': 1,
                                       'bedrooms': 1, 'beds': 1, 'number_of_reviews': 1, 'bathrooms': 1, 'price': 1,
                                       'cleaning_fee': 1, 'extra_people': 1, 'guests_included': 1, 'images.picture_url': 1,
                                       'review_scores.review_scores_rating': 1}):
    
    data1.append(i)
df1 =pd.json_normalize(data1)
updated_df1=df1.drop(5555)


# In[5]:


updated_df1


# In[6]:


import json
df1.describe()


# In[8]:


updated_df1['cleaning_fee'].fillna("not specified",inplace=True)








# In[9]:


updated_df1['beds']=updated_df1['beds'].fillna(updated_df1['beds'].mean())
updated_df1['bedrooms']=updated_df1['bedrooms'].fillna(updated_df1['bedrooms'].mean())
updated_df1['bathrooms']=updated_df1['bathrooms'].fillna(updated_df1['bathrooms'].mean())



# In[7]:


#dataype_conversion
updated_df1['bathrooms'] = updated_df1['bathrooms'].astype(str).astype(float) #run first




# In[ ]:


#Host data


# In[10]:


data2= []
for i in col.find({}, {'_id': 1,'host.host_id':1, 'host.host_name': 1,'host.host_url':1,'host.host_location':1,'host.host_response_time':1,'host.host_neighbourhood':1,'host.host_response_rate':1,'host.host_is_superhost':1,"host.host_has_profile_pic":1,'host.host_identity_verified':1,'host.host_listings_count':1,'host.host_total_listings_count':1,'host.host_verifications':1}):
    #print(type(i))
    data2.append(i)
df2=pd.json_normalize(data2)   


# In[11]:


data=[]
for i in col.find({},{"_id":1,'address.location.type':1,"address.location.coordinates":1,'address.location.is_location_exact':1}):
    data.append(i)
df5=pd.json_normalize(data)
updated_df5=df5.drop(5555)
updated_df5.isnull().sum()


# In[27]:


#address data


# In[12]:


updated_df2=df2.drop(5555)
updated_df2


# In[13]:


updated_df2["host.host_response_time"].fillna("not specified",inplace=True)
updated_df2["host.host_response_rate"].fillna("not specified",inplace=True)


# In[14]:


updated_df2.isnull().sum()


# In[15]:


data3=[]
for i in col.find({},{'_id':1,'address.street':1,'address.suburb':1,'address.government_area':1,'address.market':1,'address.country':1,'address.country_code':1}):
    data3.append(i)

df3=pd.json_normalize(data3)
df3.isnull()


# In[16]:


updated_df3=df3.drop(5555)
updated_df3.isnull().sum()


# In[17]:


#Amenities data


# In[18]:


data4=[]
for i in col.find({},{'_id':1,'amenities':1}):
    data4.append(i)
df4=pd.DataFrame(data4)
updated_df4=df4.drop(5555)
updated_df4


# In[21]:


data6=[]
for i in col.find({},{'_id':1,'availability.availability_30':1,'availability.availability_60':1,"availability.availability_90":1,'availability.availability_365':1}):
    data6.append(i)
df6=pd.json_normalize(data6)
updated_df6=df6.drop(5555)
updated_df6.isnull().sum()




# In[20]:


#merge the dataframe based on common column


# In[22]:


df=pd.merge(updated_df1,updated_df2,on="_id")
df=pd.merge(df,updated_df3,on="_id")
df=pd.merge(df,updated_df4,on="_id")
df=pd.merge(df,updated_df5,on="_id")
df=pd.merge(df,updated_df6,on="_id")


# In[23]:


df['amenities'] = df['amenities'].apply(lambda x: ', '.join(x))
df['host.host_verifications'] = df['host.host_verifications'].apply(lambda x: ', '.join(x))



# In[24]:


#df['address.location.coordinates'] = df['address.location.coordinates'].astype(str)
df['host.host_verifications']


# In[25]:


df=df.drop(["address.location.coordinates"],axis=1)


# In[26]:


df['price'] = df['price'].astype(str).astype(float).astype(int)
df['cleaning_fee'] = df['cleaning_fee'].apply(lambda x: int(
    float(str(x))) if x != 'not specified' else 'not specified')
df['extra_people'] = df['extra_people'].astype(
    str).astype(float).astype(int)
df['guests_included'] = df['guests_included'].astype(
    str).astype(int)


# In[27]:


df


# In[29]:


mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            port=3306,
                            password="Priya1234",
                            auth_plugin='mysql_native_password')
mycursor=mydb.cursor()
mycursor.execute("Create Database IF NOT EXISTS airbnb")
mycursor.execute("use airbnb")
mycursor.execute('''Create Table IF NOT EXISTS airbnb_data(
                            _id varchar(255) primary key,
                            listing_url text ,
                            name    varchar(255),
                            property_type varchar(255),
                            room_type  varchar(255),
                            bed_type varchar(255),
                            minimum_nights int,
                            maximum_nights int,
                            cancellation_policy varchar(255),
                            accommodates int,
                            bedrooms int,
                            beds int,
                            number_of_reviews int,
                            bathrooms float,
                            price int,
                            cleaning_fee varchar(255),
                            extra_people int,
                            guests_included int,
                            images text,
                            review_scores int,
                            host_id varchar(255),
                            host_url text,
                            host_name varchar(255),
                            host_location varchar(255),               
                            host_response_time varchar(255),        
                            host_neighbourhood varchar(255),       
                            host_response_rate  varchar(255),        
                            host_is_superhost  varchar(255),          
                            host_has_profile_pic varchar(255),     
                            host_identity_verified varchar(255),    
                            host_listings_count    int,     
                            host_total_listings_count int,
                            host_verifications varchar(255),         
                            address_street  varchar(255),
                            address_suburb  varchar(255),  
                            address_government_area varchar(255),
                            address_market varchar(255),
                            address_country varchar(255),
                            address_country_code varchar(255),
                            amenities mediumtext,
                            location_type varchar(255),
                            is_location_exact varchar(255),
                            availability_30 int,
                            availability_60 int,
                            availability_90 int,
                            availability_365 int)''')


# In[30]:


mycursor.executemany("insert into airbnb_data \
                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                             %s,%s,%s,%s,%s,%s)",df.values.tolist())




# In[31]:


df.values.tolist()


# In[ ]:




