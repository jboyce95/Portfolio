# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 09:34:21 2019

@author: jboyce
"""
###############################################
#REVISION HISTORY:
#    + 20200108: Changed chrome_options to options (options=chromeOptions instead of chrome_options=chromeOptions)
#           + Refactored after getting chrome_options deprecation warning
#    + 202012XX: Selenium stopped working but fixed it after reinstall...will add alternative solution using beautiful soup and requests
###############################################

#imports
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import ui
import pandas as pd
import time
import datetime
import sqlalchemy
import pyodbc
import smtplib #for sending emails
import requests
import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import os 
#import lxmlm.html as lh
 
#set url and project dir
url = r'https://www.federalreserve.gov/datadownload/Choose.aspx?rel=H15'
proj_dir = r'M:\Capital Markets\Users\Johnathan Boyce\Misc\Programming\Python\Scripts'


#BeautifulSoup grab
html = urllib.request.urlopen(url).read()
soup = BeautifulSoup(html, 'html.parser') #The soup object contains all of the HTML in the original document.
#print(soup.prettify)


#set preferences
chromeOptions = webdriver.ChromeOptions()
#need to set chrome default directory to project folder?
prefs = {'download.default_directory' : r'M:\Capital Markets\Users\Johnathan Boyce\Misc\Programming\Python\Scripts'}
chromeOptions.add_experimental_option('prefs', prefs)


#browser/driver setup - ! DO NOT EDIT !
#changed to options instead of chrome_options=chromeOptions after getting deprecation warning
driver = webdriver.Chrome(executable_path=r'M:\Capital Markets\Users\Johnathan Boyce\Misc\Programming\Python\Scripts\chromedriver.exe', options=chromeOptions) 


#open Chrome browser and open the HUD interest rate site
driver.get(url)
button = driver.find_element_by_id('FreqRequest_3') #select Monthly Averages
button.click()
button_download = driver.find_element_by_id('btnToDownload') #locate download button
button_download.click()

time.sleep(1)

button_download2 = driver.find_element_by_id('btnDownloadFile') #locate download button (after redirect resulting from clicking 'btnToDownload')
button_download2.click()


#Use pandas to read the csv downloaded from the HUD url
csv = proj_dir + r'\FRB_H15.csv'
time.sleep(5) #wait for file to download from url to folder path
df = pd.read_csv(csv, header=5) #sets column names to row 6

#optional - export the dataframe to excel
#df.to_excel(proj_dir + r'\test_output.xlsx')

#sort the dataframe
df_sort = df.sort_values('Time Period', ascending=False).reset_index()


#grab the debenture rate
debrate = df_sort.iloc[0]['RIFLGFCY10_N.M']
time_period = df_sort.iloc[0]['Time Period']
print("Most recent Debenture Rate is: {} (Time Period is {})".format(debrate, time_period))
#print(df_sort.iloc[0]['Time Period'])
#print(df_sort.columns)


#maybe add debrate method for older loans
# consider https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield

