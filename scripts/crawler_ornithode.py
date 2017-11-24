#!/usr/bin/env python3
# coding: utf8

import MySQLdb
import locale
import re
from datetime import datetime
from requests import Session
from configparser import ConfigParser
from bs4 import BeautifulSoup

# set german weekdays
loc = locale.setlocale(locale.LC_TIME, "de_DE.utf8")
# for windows
# loc = locale.setlocale(locale.LC_TIME, 'deu_deu')

# get configuration
config = ConfigParser()
config.read("/scripts/crawler_ornithode.ini")
#config.read("./crawler_ornithode.ini")
ornithouser = config.get('SectionUser', 'user')
ornithopass = config.get('SectionUser', 'pass')
dbhost = config.get('SectionDB', 'dbhost')
dbuser = config.get('SectionDB', 'dbuser')
dbpass = config.get('SectionDB', 'dbpass')
dbchar = config.get('SectionDB', 'dbchar')
dbname = config.get('SectionDB', 'dbname')

def OrnithoGetPage(s, dataurl):
    htmlsource = s.get(dataurl)
    return htmlsource

def getstartID():
    db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, charset=dbchar)
    cur = db.cursor()
    command = "SELECT max(sighting_id) FROM Sichtungen"
    cur.execute(command)
    result = cur.fetchone()[0]
    if result is None: result = 0
    return(result)

def readnewPlaces():
    # SELECT distinct(ort_id) from Sichtungen order by ort_id DESC
    db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, charset=dbchar)
    cur = db.cursor()
    command = "SELECT distinct(ort_id) from Sichtungen order by ort_id DESC"
    cur.execute(command)
    result = cur.fetchall()
    if result is None: result = []
    return(result)

def getstopID(s):
    today = datetime.today().strftime("%d.%m.%Y")
    ids = []
    dataurl = "http://www.ornitho.de/index.php?m_id=94&p_c=1&p_cc=-1&sp_tg=1&sp_DFrom=" + str(today) + "&sp_DTo=" + str(today) + "&sp_DSeasonFromDay=1&sp_DSeasonFromMonth=1&sp_DSeasonToDay=31&sp_DSeasonToMonth=12&sp_DChoice=offset&sp_DOffset=1&sp_SChoice=all&speciesFilter=&sp_S=1197&sp_Cat%5Bnever%5D=1&sp_Cat%5Bveryrare%5D=1&sp_Cat%5Brare%5D=1&sp_Cat%5Bescaped%5D=1&sp_Family=1&sp_PChoice=all&sp_cC=0000000000000000000000000000000000000000000011111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000&sp_cCO=010000000000000000000000000&sp_CommuneCounty=619&sp_Commune=17068&sp_Info=&sp_P=0&sp_Coord%5BW%5D=11.263732477302&sp_Coord%5BS%5D=47.854586363293&sp_Coord%5BE%5D=11.281701929233&sp_Coord%5BN%5D=47.872555815224&sp_AltitudeFrom=-19&sp_AltitudeTo=2962&sp_CommentValue=&sp_OnlyAH=0&sp_Ats=-00000&sp_FChoice=list&sp_FDisplay=DATE_PLACE_SPECIES&sp_DFormat=DESC&sp_FOrderListSpecies=ALPHA&sp_FListSpeciesChoice=DATA&sp_DateSynth=27.08.2017&sp_FOrderSynth=ALPHA&sp_FGraphChoice=DATA&sp_FGraphFormat=auto&sp_FAltScale=250&sp_FAltChoice=DATA&sp_FMapFormat=none&submit=Abfrage+starten"
    soup = BeautifulSoup(OrnithoGetPage(s, dataurl).text, "lxml")
    allcontent = soup.findAll("a")
    for url in allcontent:
        href = url.get("href")
        if "http://www.ornitho.de/index.php?m_id=54&id=" in href:
            ids.append(href.split("=")[2])
    ids = sorted(ids)
    return(ids[len(ids)-1])

def writesightingtoDB(table, sighting_id, date, time, melder_id, ort_id, art_id, anzahl, permalink, public):
    db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, charset=dbchar)
    cur = db.cursor()
    source = "ornitho.de"
    command = "INSERT INTO " + table + " VALUES "
    cur.execute(command + "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (sighting_id, date, time, melder_id, ort_id, art_id, anzahl, permalink, public, source))
    print("written to DB: ", sighting_id, date, time, melder_id, ort_id, art_id, anzahl, permalink, public, source)
    db.commit()

def writeplacetoDB(table, ort_id, ort_name, ort_pointeast, ort_pointnorth, ort_hoehe, ort_bundesland_kurz, ort_kreis_kurz, ort_kreis_lang):
    db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, charset=dbchar)
    cur = db.cursor()
    source = "ornitho.de"
    command = "INSERT INTO " + table + " VALUES "
    cur.execute(command + "(%s,%s,%s,%s,%s,%s,%s,%s)", (ort_id, ort_name, ort_pointeast, ort_pointnorth, ort_hoehe, ort_bundesland_kurz, ort_kreis_kurz, ort_kreis_lang))
    print("written to DB: ", ort_id, ort_name, ort_pointeast, ort_pointnorth, ort_hoehe, ort_bundesland_kurz, ort_kreis_kurz, ort_kreis_lang)
    db.commit()

def readData(allcontent, sighting_id, permalink, public):
    for item in allcontent:
        for entry in item:
            monthlist = [ "Januar ", "Februar ", u"März ", "April ", "Mai ", "Juni ", "Juli ", "August ", "September ", "Oktober ", "November ", "Dezember " ]
            if 'date' not in locals() and any(month in item.text for month in monthlist):
                # try Uhrzeit, sonst nur Datum, leere Uhrzeit
                temptime = item.text#.encode('iso-8859-1')
                try:
                      date = datetime.strptime(temptime, "%A, %d. %B %Y, %H:%M")
                      time = date.time()
                      date = date.date()
                except:
                      try: date = datetime.strptime(temptime, "%A, %d. %B %Y")
                      except: date = datetime.strptime(temptime, "%B %Y")
                      time = None
        url = item.find("a")
        try:
            #Melder_ID
            if (len(url) == 1) and ( "http://www.ornitho.de/index.php?m_id=53&amp;id=" in str(url) ):
                try: melder_id = url.get("href").split("=")[2]
                except: pass
            #Ort_ID
            if (len(url) == 1) and ( "place&amp" in str(url) ):
                try: ort_id = url.get("href").split("=")[8].split("&")[0]
                except: pass
            #Art_ID
            if (len(url) == 1) and ( "species" in str(url) ):
                try: art_id = url.get("href").split("=")[8].split("&")[0]
                except: pass
        except: pass
        #Anzahl
        if ("Anzahl" in item.text):
            try: anzahl = str(item.findNext("div").text).encode('utf8').split('\n', 1)[0]
            except: pass
    #check if variables exist
    if 'ort_id' not in locals(): ort_id = None
    if 'melder_id' not in locals(): melder_id = None
    if 'anzahl' not in locals(): anzahl = None
    if 'art_id' not in locals(): art_id = None
    writesightingtoDB("Sichtungen", sighting_id, date, time, melder_id, ort_id, art_id, anzahl, permalink, public)

# stolen from github, rewrite maybe
# converts location from or to google maps
def dms2dec(dms_str):
    dms_str = re.sub(r'\s', '', dms_str)
    if re.match('[swSW]', dms_str):
        sign = -1
    else:
        sign = 1
    (degree, minute, second, frac_seconds, junk) = re.split('\D+', dms_str, maxsplit=4)
    return sign * (int(degree) + float(minute) / 60 + float(second) / 3600 + float(frac_seconds) / 36000)

def getPlace(s, ort_id):
    dataurl = "http://www.ornitho.de/index.php?m_id=52&id=" + str(ort_id)
    soup = BeautifulSoup(OrnithoGetPage(s, dataurl).text, "lxml")
    allcontent = soup.find("table", cellpadding = 4)
    cells = allcontent.findAll("td")
    #get ort_name
    ort_name = str(cells[1]).split(">")[2].split("<")[0]
    #get ort_pointeast
    ort_pointeast = dms2dec(str(cells[1]).split(">")[5].split("/")[0])
    #get ort_pointnorth
    ort_pointnorth = (dms2dec(str(cells[1]).split(">")[5].split("/")[1].split("<")[0]))
    #get ort_hoehe
    ort_hoehe = str(cells[1]).split(">")[6].split(": ")[1].split("<")[0]
    #get ort_bundesland_kurz
    ort_bundesland_kurz = str(cells[3]).split("(")[1].split(",")[0]
    #get ort_kreis_kurz
    ort_kreis_kurz = str(cells[3]).split("(")[1].split(", ")[1].split(")")[0]
    #get ort_kreis_lang
    ort_kreis_lang = str(cells[3]).split(">")[4].split("<")[0]
    writeplacetoDB("Orte", ort_id, ort_name, ort_pointeast, ort_pointnorth, ort_hoehe, ort_bundesland_kurz, ort_kreis_kurz, ort_kreis_lang)

def main():
    area = "de"
    ornithologin = "http://www.ornitho." + area + "/index.php"
    ornithodataurl = "http://www.ornitho." + area + "/index.php?m_id=54&id="
    ornithodepayload = { "login":"1", "USERNAME":ornithouser, "REMEMBER":"ON", "PASSWORD":ornithopass }
    startid = int(getstartID()) + 1
    #login
    s = Session()
    response = s.post(ornithologin, data=ornithodepayload)
    stopid = int(getstopID(s))
    # get new sightings
    while (startid <= stopid):
        #get page
        dataurl = ornithodataurl + str(startid)
        soup = BeautifulSoup(OrnithoGetPage(s, dataurl).text, "lxml")
        allcontent = soup.findAll("div", { "class" : re.compile("^(col-xs-4|col-xs-8)$")})
        sighting_id = startid
        permalink = dataurl
        if (len(allcontent) == 0):
                public = 0
                writesightingtoDB("Sichtungen", sighting_id, None, None, None, None, None, None, permalink, public)
        else:
                public = 1
                readData(allcontent, sighting_id, permalink, public)
        startid+=1
    # get new places
    places = readnewPlaces()
    for place in places:
        getPlace(place)

if __name__ == "__main__":
    main()
