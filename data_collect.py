#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 21:57:01 2016

@author: Michel HUNSICKER
"""
#system import
import os
import sys

# Internet access
import urllib
import urllib.request
import urllib.parse
import requests

# Internet data handling imports
from bs4 import BeautifulSoup

# Data base and initial data import
import sqlite3
import csv
from datetime import datetime, date
import codecs

#Required for test
import time

#Main variables
INITDB = False #initalise the database False for no or True for yes. 
initDate = date(2012, 1, 1) #date.today() if the initialisation date is set today.
DEBUG = False #Allows more verbose in debugging mode

# Intenet access variables
login = "MyLogin"
password = "MyPassword"
proxyAuthentification = login + ":" + urllib.parse.quote(password) + "@"
proxyNameList = ["1", "2"]
workingProxy=""  #When a proxy is working it is used preferably
proxyNeeded = False
proxySet = False  #Define wheter a working proxy has been defined. 

# Database and initial data import
filePath= os.path.dirname(os.path.realpath(__file__))
companyImportFile = os.path.join(filePath,"Data", "PortfolioRTF.csv")
adTypeImportFile = os.path.join(filePath,"Data", "BodaccAddType.csv")
dataBaseName = os.path.join(filePath,"Data", "bodaccAlert.db")
companyTable = "COMPANIES"
adTypeTable="AD_TYPE"
adTable="ADS"

#Email adress where messages should be sent to
emailAdress=""
#Mailgun authentication
MailServerKey=""
MailServerAdress=""


def InitDB():
    print("DB initalization phase started")
    connection = sqlite3.connect(dataBaseName,detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    # Table creations
    connection.executescript ('''DROP TABLE IF EXISTS {CT}; CREATE TABLE {CT} (SIREN int PRIMARY KEY, name textE)'''.format(CT=companyTable))
    connection.commit()   
    
    connection.executescript ('''DROP TABLE IF EXISTS {ATT}; CREATE TABLE {ATT} (id INTEGER PRIMARY KEY AUTOINCREMENT, description textE ,BODACCCode textE)'''.format(ATT=adTypeTable))    
    connection.commit()
    
    connection.executescript ('''DROP TABLE IF EXISTS {AT}; CREATE TABLE {AT} (SIREN int, type INTEGER, publicationDate date, description textE, FOREIGN KEY (SIREN) REFERENCES {CT}(SIREN), FOREIGN KEY (type) REFERENCES {ATT}(codeId))'''.format(AT=adTable, CT=companyTable, ATT=adTypeTable))    
    connection.commit()

    #File imports
    CSVFile = codecs.open(companyImportFile, 'r','utf-8')
    fileReader = csv.reader(CSVFile, delimiter=';')    
    #Jump the title line (first line of the file)
    fileReader.__next__()
    #Iterate over all file lines. Field 1 : SIREN, Field 2: Company Name, Field 3: last 
    for row in fileReader:
        connection.execute ("INSERT INTO " + companyTable + ''' VALUES (?, ?) ''',(row[0], row[1]))        
    connection.commit()
    
    CSVFile = codecs.open(adTypeImportFile, 'r','utf-8')
    fileReader = csv.reader(CSVFile, delimiter=';')    
    #Jump the title line (first line of the file)
    fileReader.__next__()
    #Iterate over all file lines. Field 1 : auto increment ID, Field 2: Description, Field3: BODACCCode} 
    for row in fileReader:
        connection.execute ("INSERT INTO " + adTypeTable +"(BODACCCode, Description)" +''' VALUES (?, ?) ''',(row[0], row[1]))        
    connection.commit()
    
    connection.close()  
    print("DB initialized !")        


class SendMailError(Exception):
    def __init__(self, value):
        self.value = value
    def __str_(self):
        return repr(self.value)


def SendEmail(receiver, subject , text):
#Tested with mailgun API
   request = requests.post(MailServerAdress, auth=('api', MailServerKey), data={
        'from': 'BodaccAlertScanner@mailgun.net',
        'to': receiver,
        'subject': subject,
        'text': text
        })
   if DEBUG:
       print('Status: {0}'.format(request.status_code))
       print('Body:   {0}'.format(request.text))
   if request.status_code != 200:
       raise SendMailError('Erreur envoi mail cf. code retour {0}'.format(request.status_code))        

    
def GetURL(baseURL, params=None):
#Init the internet access 
    global proxySet
    global proxyNeeded
    if proxyNeeded: 
        if proxySet : 
            if params==None:
                return urllib.request.urlopen(baseURL)
            else: 
                return urllib.request.urlopen(baseURL,params) 
        else:
            for proxy in proxyNameList:    
                httpChain = "http://"+proxy+":3128"
                httpsChain = "https://"+proxy+":3128"
                try: 
                    proxyHandler = urllib.request.ProxyHandler({"https":httpsChain, "http":httpChain}) 
                    pwdMgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
                    pwdMgr.add_password(None, proxy, login, password)
                    proxyAutHandler = urllib.request.ProxyBasicAuthHandler(pwdMgr)
                    opener = urllib.request.build_opener(proxyHandler,proxyAutHandler)
                    urllib.request.install_opener(opener)
                    if params==None:
                        webPage = urllib.request.urlopen(baseURL)
                        proxySet = True
                        return webPage
                    else: 
                        webPage = urllib.request.urlopen(baseURL)
                        proxySet = True
                        return webPage
                except Exception as e:
                    print("Erreur: " + str(e) + " avec proxy " + proxy)
        #If no proxy is working we need to abort the process
            raise NameError("No proxy allowed internet request")
                    
    else: 
        if params==None:
            return urllib.request.urlopen(baseURL)
        else: 
            return urllib.request.urlopen(baseURL,params)    
    
def CheckAlert(adTypeCode):
#The adTypeCode data is supposed to be the primary key of the AdType table.     
    if DEBUG:
        print('''Procédure de récupération des alertes démarrée''')
    #Init data connection
    connection = sqlite3.connect(dataBaseName,detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)   
    #Prepare dataType elements. 
    cursor = connection.execute ('''SELECT BODACCCode, description from {ATT} WHERE id=?'''.format(ATT= adTypeTable),str(adTypeCode))
    adType = cursor.fetchone()
    cursor.close()       
    BODACCAdTypeCode= adType[0]
    AdTypeLabel = adType[1]    
    
    #Récupérer SIREN plus dernières dates de publication pour le type d'adType).     
    table= connection.execute ('''SELECT SIREN,Name from {CT}'''.format(CT= companyTable))
    table=list(table)
    #ans doute à améliorer via du SQL (cf. la fonction MAX dnas le SELECT ne permet pas de récupérer un format de date)
    for i in range(len(table)):
        cursor = connection.execute ('''SELECT publicationDate from {AT} WHERE SIREN=? AND type = ?'''.format(AT= adTable),(table[i][0],str(adTypeCode)))
        dateList =  cursor.fetchall()
        cursor.close()        
        bufferList  = list(table[i])     
        if dateList:
            datePublication = max(i[0] for i in dateList )
            bufferList.append(datePublication)
        else:
            bufferList.append(initDate)
        table[i]= bufferList     
            
    #Define cumulated ad text for one company and one ad type MUST BE REPLACE BY A DB QUERY
    adsFormatedText=''
    
    for row in table: 
        SIREN =  row[0]
        companyName = row[1]
        lastDate = row[2]
        publicationDateList =[]
        URLParameters = urllib.parse.urlencode({ "registre" : SIREN ,\
                                                "categorieannonce" : BODACCAdTypeCode, \
                                                "datepublicationmin" : lastDate.strftime("%d/%m/%Y") } )                            
        URLParameters=URLParameters.encode('utf-8')      
        searchResult = GetURL("http://www.bodacc.fr/annonce/liste?action=send", URLParameters)
        adResultList = BeautifulSoup(searchResult,'html.parser')
        adList=adResultList.find_all("tr", class_="pair") + adResultList.find_all("tr", class_="impair")
        for ad in adList: 
            #Format the date of the ad in plain text into subelemnent to built a Python date.
            dateElements=ad.td.text.split("/")
            publicationDate = date(int(dateElements[2]), int(dateElements[1]), int(dateElements[0]))
            if publicationDate > lastDate:
                publicationDateList.append(publicationDate)
                #print("Alerte Bodacc " + companyName + " " + publicationDate.isoformat() + ":")        
                adLink = ad.find('a').get('href')
                adPage=GetURL("http://www.bodacc.fr"+adLink)                        
                adContent= BeautifulSoup(adPage,'html.parser')
                adTextZone=adContent.find("div", id="annonce")
                adText = adTextZone.find("em").get_text() + "\n"
                #for element in adTextZone.find("dl").find_all(recursive=False):
                for element in adTextZone.dl.find_all(recursive=False):
                    if element.get_text() == "" :
                        pass
                    elif element.name == "dt":
                        adText = adText + ' '.join(element.get_text().split())  + ' '
                    else: 
                        adText = adText + ' '.join(element.get_text().split()) + "\n"
                #Save to database                
                connection.execute ('''INSERT INTO {AT} VALUES(?,?,?,?)'''.format(AT=adTable), (SIREN, str(adTypeCode),publicationDate,adText))  
                connection.commit()
                #Result saving in a file    
                adsFormatedText+="*********************************************************\n"
                adsFormatedText+="Alerte Bodacc " + companyName + " " + publicationDate.isoformat() + ":\n"             
                adsFormatedText+=adText + "\n"
                #if adTypeCode==1:
                SendEmail(emailAdress, "Alerte Bodacc "+ companyName, adText)
            
    
        #Console output
        if DEBUG:  
            if publicationDateList == []:
                print("Research done for " + companyName + ": No new ad regarding type " + AdTypeLabel)
            else: 
                publicationDateList.sort(reverse=True)
                lastDate = publicationDateList[0]                
                print("Research done for " + companyName + ": Warning new ad(s) regarding type " + AdTypeLabel)
                
    #Save results for all companies in file    
    if adsFormatedText =='':
        if DEBUG:  
            outputFile = open(os.path.join(filePath,"AlertBodacc_" + AdTypeLabel + " " + datetime.now().strftime("%Y-%m-%dT%H%M") + "_OK.txt"), "w") 
            outputFile.write("No alerts detected")
            outputFile.close()
        print('''Pas de nouvelle alerte concernant le type '''+ AdTypeLabel)

    else:
        if DEBUG: 
            outputFile = open(os.path.join(filePath, "AlertBodacc_" + AdTypeLabel + " "  + datetime.now().strftime("%Y-%m-%dT%H%M") + "_WARNING.txt"), "w")          
            outputFile.write(adsFormatedText)
            outputFile.close()
            print('''Attention nouvelle(s) alerte(s) '''+ AdTypeLabel)
        
    #Print console message
    if DEBUG: 
        print('''Procédure de récupération des alertes''' + AdTypeLabel + ''' terminée !''')
    connection.commit()
    connection.close()                


#Lancemenent 

def collectData():
    try: 
        print("******************Récupération du " + datetime.now().strftime("%Y-%m-%dT%H%M")+"***************")
        #Initialize the code list
        connection = sqlite3.connect(dataBaseName,detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.execute ('''SELECT id from {ATT}'''.format(ATT= adTypeTable))
        adTypeList = list(cursor) 
        connection.close()
        for type in adTypeList: 
                 CheckAlert(type[0])     
        SendEmail(emailAdress, "Update of your local Bodacc database was sucessful","Well done")    
        time.sleep(15)
    except Exception as e:
        print("Failure during ads collection process: " + str(e))
        outputFile = open(os.path.join(filePath, "AlertBodacc" + datetime.now().strftime("%Y-%m-%dT%H%M") + "_ERREUR.txt"), "w")          
        outputFile.write(str(e))    
        outputFile.close()
        SendEmail(emailAdress, "Update of you local Bodacc database failed","due to the following error :" + str(e))


def main(myEmailAdress, myMailServerAdress,myMailServerKey,myDebug=False):
    global emailAdress 
    emailAdress = myEmailAdress
    
    global MailServerAdress 
    MailServerAdress = myMailServerAdress

    global MailServerKey 
    MailServerKey = myMailServerKey
    
    global DEBUG
    DEBUG=myDebug

    if INITDB :
        InitDB()  
    
    collectData()

if __name__ == "__main__":
    main(sys.argv[1:])

