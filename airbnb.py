import pandas as pd
import pymongo
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu
from PIL import Image
import warnings
warnings.filterwarnings("ignore")
st.set_page_config(page_title= "Airbnb Data Visualization | Priyanka Pal",
                   #page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This dashboard app is created by *Priyanka Pal*!
                                        Data has been gathered from mongodb atlas"""}
                  )
with st.sidebar:


    image_url =Image.open(r"C:\Users\ADMIN\Downloads\airbnb_logo.png")
    st.image(image_url, use_column_width=True)
    selected = option_menu(menu_title="", options=["Home", "Visualization","Exit"], 
                          icons=['house','bar-chart', 'sign-turn-right-fill'],
                          styles={
                               "container": {"padding": "0!important", "font-family": "Permanent Marker"},
                               "icon": {"color": "orange", "font-size": "15px"}, 
                               "nav-link": {"font-size": "15px", "text-align": "left", "margin":"0px"},
                               "nav-link-selected": {"background-color": "red"},
                               }
                              )
if selected=='Home':
    
    st.subheader(":red[Welcome to the Homepage of Airbnb]")

    st.subheader(":violet[**Domain:**] Travel Industry, Property Management and Tourism") 

    st.markdown(":violet[Airbnb, Inc. is an American San Francisco-based company operating an online marketplace for short- and long-term homestays and experiences. The company acts as a broker and charges a commission from each booking.]")
    
    st.write(":green[Airbnb has revolutionized the travel and property management industry, making it crucial to analyze its data to gain insights into pricing, availability patterns, and location-based trends]")
    
    st.write("Explore the Tableau Dashboard https://public.tableau.com/views/airbnbdashboard_16955776524000/Dashboard1?:language=en-GB&:display_count=n&:origin=viz_share_link for in-depth insights")

if selected == "Visualization":
    file=st.file_uploader("Upload a file",type=(["csv","txt","xlsx"]))
    if file is not None:
        filename=file.name
        st.write(filename)
        df=pd.read_csv(filename)
        tab1,tab2 = st.tabs(["Explore(Listings based on Property type,Room type,Host Name,Country)", "Explore(Price Analysis & Availability Analysis by room type & Country)"])
        with tab1:
            country = st.sidebar.multiselect('Select a Country',sorted(df.Country.unique()))
            prop = st.sidebar.multiselect('Select Property_type',sorted(df.property_type.unique()))
            room = st.sidebar.multiselect('Select Room_type',sorted(df.room_type.unique()))
            price = st.slider('Select Price',df.price.min(),df.price.max(),(df.price.min(),df.price.max()))

            # CONVERTING THE USER INPUT INTO QUERY
            query = f'Country in {country} & room_type in {room} & property_type in {prop} & price>={price[0]} and price<={price[1]}'
            
            # CREATING COLUMNS
            col1,col2 = st.columns(2,gap='medium')
            
            with col1:
                
                # TOP 10 PROPERTY TYPES BAR CHART
                df1 = df.query(query).groupby(["property_type"]).size().reset_index(name="Listings").sort_values(by='Listings',ascending=False)[:10]
                fig = px.bar(df1,
                             title='Total Listings of selected Property Types',
                             x='Listings',
                             y='property_type',
                             orientation='h',
                             color='property_type',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig,use_container_width=True) 
            
                # TOP 10 HOSTS BAR CHART
                df2 = df.query(query).groupby(["host_name"]).size().reset_index(name="Listings").sort_values(by='Listings',ascending=False)[:10]
                fig = px.bar(df2,
                             title='Top 10 Hosts with Highest number of Listings',
                             x='Listings',
                             y='host_name',
                             orientation='h',
                             color='host_name',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig,use_container_width=True)
            
            with col2:
                
                # TOTAL LISTINGS IN EACH ROOM TYPES PIE CHART
                df1 = df.query(query).groupby(["room_type"]).size().reset_index(name="counts")
                fig = px.pie(df1,
                             title='Total Listings of selected Room_types',
                             names='room_type',
                             values='counts',
                             color_discrete_sequence=px.colors.sequential.Rainbow
                            )
                fig.update_traces(textposition='outside', textinfo='value+label')
                st.plotly_chart(fig,use_container_width=True)
                
                # TOTAL LISTINGS BY COUNTRY CHOROPLETH MAP
                country_df = df.query(query).groupby(['Country'],as_index=False)['name'].count().rename(columns={'name' : 'Total_Listings'})
                fig = px.choropleth(country_df,
                                    title='Total Listings in each Country',
                                    locations='Country',
                                    locationmode='country names',
                                    color='Total_Listings',
                                    color_continuous_scale=px.colors.sequential.Plasma
                                   )
                st.plotly_chart(fig,use_container_width=True)
        with tab2:
            #country = st.sidebar.multiselect('Select a Country',sorted(df.Country.unique()),sorted(df.Country.unique()))
            #prop = st.sidebar.multiselect('Select Property_type',sorted(df.property_type.unique()),sorted(df.property_type.unique()))
            #room = st.sidebar.multiselect('Select Room_type',sorted(df.room_type.unique()),sorted(df.room_type.unique()))
            #price = st.slider('Select price',df.price.min(),df.price.max(),(df.price.min(),df.price.max()))
            
            # CONVERTING THE USER INPUT INTO QUERY
            query = f'Country in {country} & room_type in {room} & property_type in {prop} & price >= {price[0]} & price <= {price[1]}'
            
            # HEADING 1
            st.markdown("## Price Analysis")
            
            # CREATING COLUMNS
            col1,col2 = st.columns(2,gap='medium')
            
            with col1:
                
                # AVG PRICE BY ROOM TYPE BARCHART
                pr_df = df.query(query).groupby('room_type',as_index=False)['price'].mean().sort_values(by='price')
                fig = px.bar(data_frame=pr_df,
                             x='room_type',
                             y='price',
                             color='price',
                             title='Avg Price of selected Room type'
                            )
                st.plotly_chart(fig,use_container_width=True)
                
                # HEADING 2
                st.markdown("## Availability Analysis")
                
                # AVAILABILITY BY ROOM TYPE BOX PLOT
                fig = px.box(data_frame=df.query(query),
                             x='room_type',
                             y='availability_365',
                             color='room_type',
                             title='Availability by Room_type'
                            )
                st.plotly_chart(fig,use_container_width=True)
                
            with col2:
                
                # AVG PRICE IN COUNTRIES SCATTERGEO
                country_df = df.query(query).groupby('Country',as_index=False)['price'].mean()
                fig = px.scatter_geo(data_frame=country_df,
                                               locations='Country',
                                               color= 'price', 
                                               hover_data=['price'],
                                               locationmode='country names',
                                               size='price',
                                               title= 'Avg Price in each Country',
                                               color_continuous_scale='agsunset'
                                    )
                col2.plotly_chart(fig,use_container_width=True)
                
                # BLANK SPACE
                st.markdown("#   ")
                st.markdown("#   ")
                
                # AVG AVAILABILITY IN COUNTRIES SCATTERGEO
                country_df = df.query(query).groupby('Country',as_index=False)['availability_365'].mean()
                country_df.availability_365 = country_df.availability_365.astype(int)
                fig = px.scatter_geo(data_frame=country_df,
                                               locations='Country',
                                               color= 'availability_365', 
                                               hover_data=['availability_365'],
                                               locationmode='country names',
                                               size='availability_365',
                                               title= 'Avg Availability in each Country',
                                               color_continuous_scale='agsunset'
                                    )
                st.plotly_chart(fig,use_container_width=True)


if selected=="Exit":
    st.text("")
    st.success('Thank you for your time. Exiting the application')
    st.balloons()
