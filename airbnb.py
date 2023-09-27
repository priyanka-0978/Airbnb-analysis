import pandas as pd
import pymongo
import mysql.connector
import plotly.express as px
import json
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import time
import numpy as np
from PIL import Image
#------------------------------setting page configuaration-------------------------------------------
st.set_page_config(page_title= "Airbnb",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About':"This dashboard app is created by Priyanka Pal!"}
                   )
st.markdown(f'<h1 style="text-align: center;">Airbnb Analysis</h1>',
                unsafe_allow_html=True)
# ---------------------------------Creating Dashboard----------------------
with st.sidebar:
   image_url =Image.open(r"C:\Users\Priyanka\Downloads\airbnb_logo.png")
   st.image(image_url, use_column_width=True)

   selected = option_menu(menu_title="", options=["Home", "Migrate Data to SQL","Search by Category","Visualizations","Exit"], 
                          icons=['house','database-fill', 'list-task', 'bar-chart', 'sign-turn-right-fill'],
                          styles={
                               "container": {"padding": "0!important", "font-family": "Permanent Marker"},
                               "icon": {"color": "orange", "font-size": "15px"}, 
                               "nav-link": {"font-size": "15px", "text-align": "left", "margin":"0px"},
                               "nav-link-selected": {"background-color": "red"},
                               }
                              )
#---------------------------------Extracting data from mongodb--------------------------------------------------------#

client= pymongo.MongoClient("mongodb+srv://priya123:FLAXnahS95SgJLHA@cluster0.uw7a71f.mongodb.net/test?authMechanism=DEFAULT")
db = client['sample_airbnb']
col=db['listingsAndReviews']

data1= []
for i in col.find({}, {'_id': 1, 'listing_url': 1, 'name': 1, 'property_type': 1, 'room_type': 1, 'bed_type': 1,
                                       'minimum_nights': 1, 'maximum_nights': 1, 'cancellation_policy': 1, 'accommodates': 1,
                                       'bedrooms': 1, 'beds': 1, 'number_of_reviews': 1, 'bathrooms': 1, 'price': 1,
                                       'cleaning_fee': 1, 'extra_people': 1, 'guests_included': 1, 'images.picture_url': 1,
                                       'review_scores.review_scores_rating': 1}):

    data1.append(i)
df1 =pd.json_normalize(data1)

updated_df1=df1.drop(5555)
updated_df1['bathrooms'] = updated_df1['bathrooms'].astype(str).astype(float)
updated_df1['cleaning_fee'].fillna("not specified",inplace=True)
updated_df1['beds']=updated_df1['beds'].fillna(updated_df1['beds'].mean())
updated_df1['bedrooms']=updated_df1['bedrooms'].fillna(updated_df1['bedrooms'].mean())
updated_df1['bathrooms']=updated_df1['bathrooms'].fillna(updated_df1['bathrooms'].mean())
updated_df1['review_scores.review_scores_rating']=updated_df1['review_scores.review_scores_rating'].fillna(updated_df1['review_scores.review_scores_rating'].mean())

data2= []
for i in col.find({}, {'_id': 1,'host.host_id':1, 'host.host_name': 1,'host.host_url':1,'host.host_location':1,'host.host_response_time':1,'host.host_neighbourhood':1,'host.host_response_rate':1,'host.host_is_superhost':1,"host.host_has_profile_pic":1,'host.host_identity_verified':1,'host.host_listings_count':1,'host.host_total_listings_count':1,'host.host_verifications':1}):
    data2.append(i)
df2=pd.json_normalize(data2)   
updated_df2=df2.drop(5555)
updated_df2["host.host_response_time"].fillna("not specified",inplace=True)
updated_df2["host.host_response_rate"].fillna("not specified",inplace=True)

data3=[]
for i in col.find({},{'_id':1,'address.street':1,'address.suburb':1,'address.government_area':1,'address.market':1,'address.country':1,'address.country_code':1}):
    data3.append(i)

df3=pd.json_normalize(data3)
updated_df3=df3.drop(5555)

data4=[]
for i in col.find({},{'_id':1,'amenities':1}):
    data4.append(i)
df4=pd.DataFrame(data4)
updated_df4=df4.drop(5555)

data5=[]
for i in col.find({},{"_id":1,'address.location.type':1,'address.location.is_location_exact':1}):
    data5.append(i)
df5=pd.json_normalize(data5)
updated_df5=df5.drop(5555)

data6=[]
for i in col.find({},{'_id':1,'availability.availability_30':1,'availability.availability_60':1,"availability.availability_90":1,'availability.availability_365':1}):
    data6.append(i)
df6=pd.json_normalize(data6)
updated_df6=df6.drop(5555)

data7=[]
for i in col.find({}, {'_id': 1,'address.location.coordinates':1}):
    data7.append(i)
df7=pd.json_normalize(data7)
updated_df7=df7.drop(5555) #there is only one null values in the given dataset
longitude=[]
for i in range(5555):
    longitude.append(updated_df7['address.location.coordinates'][i][0])
latitude=[]
for i in range(5555):
    latitude.append(updated_df7['address.location.coordinates'][i][1])

df7= pd.DataFrame(list(zip(longitude,latitude)), columns=['Longitude', 'Latitude'])
coordinates_df=updated_df7.merge(df7,left_index=True,right_index=True)
coordinates_df=coordinates_df.drop(["address.location.coordinates"],axis=1)
#---------------------------------------------merging the dataframe----------------------------------
df=pd.merge(updated_df1,updated_df2,on="_id")
df=pd.merge(df,updated_df3,on="_id")
df=pd.merge(df,updated_df4,on="_id")
df=pd.merge(df,updated_df5,on="_id")
df=pd.merge(df,updated_df6,on="_id")
df=pd.merge(df,coordinates_df,on="_id")
#----------------------------converting datatype to insert to mysql easily-----------------------------
df['amenities'] = df['amenities'].apply(lambda x: ', '.join(x))
df['host.host_verifications'] = df['host.host_verifications'].apply(lambda x: ', '.join(x))
df['price'] = df['price'].astype(str).astype(float).astype(int)
df['cleaning_fee'] = df['cleaning_fee'].apply(lambda x: int(
    float(str(x))) if x != 'not specified' else 'not specified')
df['extra_people'] = df['extra_people'].astype(
    str).astype(float).astype(int)
df['guests_included'] = df['guests_included'].astype(
    str).astype(int)
#----------------------------------------------HOMEPAGE------------------------------------------------------
if selected=='Home':
    
    st.subheader(":red[Welcome to the Homepage of Airbnb]")

    st.subheader(":violet[**Domain:**] Travel Industry, Property Management and Tourism") 

    st.markdown(":violet[Airbnb, Inc. is an American San Francisco-based company operating an online marketplace for short- and long-term homestays and experiences. The company acts as a broker and charges a commission from each booking.]")
    
    st.write(":green[Airbnb has revolutionized the travel and property management industry, making it crucial to analyze its data to gain insights into pricing, availability patterns, and location-based trends]")
    
    st.write("Explore the Tableau Dashboard https://public.tableau.com/views/airbnbdashboard_16955776524000/Dashboard1?:language=en-GB&:display_count=n&:origin=viz_share_link for in-depth insights")
#---------------------------------------UPLOADING DATA TO MYSQL-----------------------------------   
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
                            availability_365 int,
                            Longitude float,
                            Latitude float)''')

if selected=='Migrate Data to SQL':
    st.text("")
    st.write("To upload the data to sql database.Please Click the below button.")
    submit=st.button("Migrate Data to SQL")
    if submit:
        with st.spinner("Connecting to Database...."):
            time.sleep(5)
            mycursor.executemany("insert into airbnb_data \
                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                             %s,%s,%s,%s,%s,%s,%s,%s)",df.values.tolist())
            mydb.commit()
            st.success("Migrated to SQL Database Successfully!!")
#-----------------------------------------------------------------------------------#            
if selected=='Search by Category':
    mycursor.execute("SHOW COLUMNS FROM airbnb_data")
    columns = mycursor.fetchall()
    column_names=[i[0] for i in columns]
    category = st.selectbox("Choose a Category", column_names)
    query="select distinct({}) from airbnb_data".format(category)
    mycursor.execute(query)
    val=mycursor.fetchall()
    search=[i[0] for i in val]
    category_values= st.selectbox("Choose a Category values to know related information", search)
    if st.button("submit"):
        mydata=(category_values,)
        query=f"select * from airbnb_data where {category}=%s"
        mycursor.execute(query,mydata)
        val=mycursor.fetchall()
        df=pd.DataFrame(val,columns=mycursor.column_names)
        df.index=np.arange(1,len(df)+1)
        st.write(df)
#----------------------------------------------------------------------------------------------------------------#        
if selected=="Visualizations":
    mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            port=3306,
                            password="Priya1234",
                            auth_plugin='mysql_native_password',
                            database='airbnb')
    mycursor=mydb.cursor(buffered=True)
    tab1,tab2,tab3,tab4,tab5,tab6,tab7,tab8=st.tabs(["Property Type Analysis","Room Type Analysis","Host Analysis","Cancellation Policy","Price vs Reviews","Top 10 Busiest host","Coordinates-Avg Price","Country vs Count of Room Type"])
    with tab1:
        col1,col2=st.columns(2)
        with col1:
            mycursor.execute("select distinct(Property_Type),avg(price) as Price from airbnb_data group by Property_Type")
            mysql_data=mycursor.fetchall()
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            df.reset_index(drop=True)
            fig = px.bar(df,  x=df["Property_Type"],y=df['Price'],color="Property_Type",title="Analyzing the Property Type based on average Prices(in$)")
            st.plotly_chart(fig,use_container_width=True)
        with col2:
            mycursor.execute("select distinct(Property_Type),sum(number_of_reviews) as Number_of_Reviews from airbnb_data group by Property_Type order by Number_of_Reviews desc limit 10")
            mysql_data=mycursor.fetchall()
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            df.reset_index(drop=True)
            fig = px.bar(df,x=df['Property_Type'],y=df["Number_of_Reviews"],color="Property_Type", title="Analyzing the Property Type based on Number of Reviews")
            st.plotly_chart(fig,use_container_width=True)
            
        st.write(":red[Conclusions:]:green[ From the above chart we can observe that Propert Type like Heritage hotel(India) & Houseboat charges higher prices whereas others are charging approximately very less therefore we can conclude that Property Type like Apartment,House..are more preferrable as they are charging reasonable amount]")

    with tab2:
        col1,col2=st.columns([3,2])
        with col1:
            mycursor.execute("select distinct(Room_Type),avg(price) as Price from airbnb_data group by Room_Type")
            mysql_data=mycursor.fetchall()
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            df.reset_index(drop=True)
            fig = px.bar(df,x=df['Price'],  y=df["Room_Type"],color="Room_Type", title="Analyzing the Property Type based on average Prices(in$)",orientation='h')

            st.plotly_chart(fig,use_container_width=True)
        with col2:
            mycursor.execute("select room_type, sum(number_of_reviews) as reviews  from airbnb_data group by room_type")
            mysql_data=mycursor.fetchall()
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            fig = px.pie(df, values='reviews',
                         names='room_type',
                         title='Room type according to the sum of reviews')
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
            
        st.write(":red[Conclusions:]:green[ We can observe that why the number of reviews is maximum for Entire home/apt because first choice of customers is Entire room/apt and second is private room ,additionaly we can also observe that price for shared room type is more compare to others]")
        
    with tab3:
        col1,col2=st.columns([3.5,2])
        with col1:
            mycursor.execute("select host_name,avg(host_listings_count) as average_listings from airbnb_data group by host_name order by   average_listings  desc limit 10")
            mysql_data=mycursor.fetchall()
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            fig = px.bar(df,x=df['average_listings'], y=df["host_name"],color="host_name", title="Most Popular Host Name  According to the average Listing Counts",orientation='h')
            st.plotly_chart(fig,use_container_width=True)
        with col2:
            mycursor.execute("select host_name,room_type,avg(host_listings_count) as average_listings from airbnb_data group by host_name,room_type order by average_listings  desc limit 10")
            mysql_data=mycursor.fetchall()
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            df.index=np.arange(1,len(df)+1)
            st.write(df)

        st.write(":red[Conclusions:]:green[ Analysis is done on host name & host id because host name can be repeated but host id's are unique for every host.so by comparing both the chart & data  in above figure we can conclude that Popular Host name is Sonder  having average listing counts=1198 and his listed room type is Entire home/apt which is more preferrable...]")

    with tab4:
        mycursor.execute("select cancellation_policy,count(cancellation_policy) as count from airbnb_data group by cancellation_policy order by count asc limit 10")
        mysql_data=mycursor.fetchall()
        df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
        fig = px.line(df,x=df["cancellation_policy"],y=df['count'],title="Cancellation Policy Preferred by most of the Host" )
        st.plotly_chart(fig,use_container_width=True)
      
    with tab5:
        mycursor.execute("select price, count(number_of_reviews) as reviews  from airbnb_data group by price order by price desc")
        mysql_data=mycursor.fetchall()
        df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
        fig=px.scatter(df,x=df['price'],y=df['reviews'],title="Price vs Number of Reviews")
        st.plotly_chart(fig,use_container_width=True)
        st.write(":red[conclusion:]:green[ From the above visualization we can say that most number of people like to stay in less price and their reviews are higher in those areas]")
      
    with tab6:
        col1,col2=st.columns([2,3.5])
        with col1:
            mycursor.execute("select host_id, host_name,room_type,count(number_of_reviews) as reviews  from airbnb_data group by host_id,host_name,room_type order by reviews desc limit 10")
            mysql_data=mycursor.fetchall() 
            df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
            df.index=np.arange(1,len(df)+1)
            st.write(df)
        with col2:
            fig = px.bar(df, x=df["host_name"],y=df['reviews'],color="host_name", title="Analyzing the top 10 busiest host in terms of reviews")
            st.plotly_chart(fig,use_container_width=True)
        st.write(":red[Conclusion:]:green[ From the above data and chart, we can visualize that,these hosts listed their rooms type as Entire home/apartment & Private room which is preferred by most number of peoples,their reviews are higher in those areas and therefore they fall under the list of busiest hosts]")
      
    with tab7:
        mycursor.execute("select latitude,longitude,price from airbnb_data having price<=1500 ")
        mysql_data=mycursor.fetchall()
        df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
        fig=px.scatter(df,x=df['longitude'],y=df['latitude'],color=df['price'],title="Analyzing the price based on coordinates(Latitude,Longitude)")
        st.plotly_chart(fig,use_container_width=True)

        st.write(":red[Conclusion:]:green[ Performed Analysis on how the price in different area would be based on the latitude & longitude Coordinates and we can obeserve that very few areas have airbnb more than 800$]")

    with tab8:
        mycursor.execute("select address_country as Country,room_type,count(room_type) as Count from airbnb_data group by Country,room_type ")
        mysql_data=mycursor.fetchall()
        df=pd.DataFrame(mysql_data,columns=mycursor.column_names)
        fig=px.scatter(df,x=df['Country'],y=df['Count'],color=df['room_type'],title="Country Vs Count of Room Type")
        st.plotly_chart(fig,use_container_width=True)
#----------------------------------------Exit page-----------------------------------------------------
if selected=="Exit":
    st.text("")
    st.success('Thank you for your time. Exiting the application')
    st.balloons()





















