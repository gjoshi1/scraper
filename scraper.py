'''------------------------------------------------------------------------
Name:        scraper.py
Purpose:     To crawl the itunes app store US pages
Author:      Gowri Joshi

Created:     Aug 2018
--------------------------------------------------------------------------
'''

import sys
import csv
import json
import re

from bs4 import BeautifulSoup
import requests
import numpy as np
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count

all_apps_data = []
filtered_apps_data = {
                    "apps_in_spanish_and_tagalog": [],
                    "apps_with_insta_in_name": []
                    }


def main():
    if(len(sys.argv) == 1):
        print ("Please enter CSV path")
        sys.exit(1)
    #read csv into dictionary
    csv_dict = readFile(sys.argv[1])
    '''
    imap(func, iterable[, chunksize])

    A lazier version of map().
    The chunksize argument is the same as the one used by the map() method. 
    For very long iterables using a large value for chunksize can make 
    the job complete much faster than using the default value of 1.
    '''
    FILE_LINES = 10000000
    NUM_WORKERS = cpu_count() * 2  # Creates a Pool with cpu_count * 2 threads.
    chunksize = FILE_LINES // NUM_WORKERS * 4   
    pool = Pool(NUM_WORKERS)
   
    result_iter = pool.imap(scrapeURL, csv_dict, chunksize)
        
    for result in result_iter:  # lazily iterate over results.
        all_apps_data.append(result)
        filterApps(result)
    '''
    apps_with_insta_in_name-array of numbers, sorted ascending -
        App identifiers of all apps apps that have “insta” in the name (case insensitive)
    apps_in_spanish_and_tagalog - array of numbers, sorted ascending -
        App identifiers of all apps that are available in both Spanish and Tagalog
    '''
    np.array(filtered_apps_data['apps_with_insta_in_name']).sort()
    np.array(filtered_apps_data['apps_in_spanish_and_tagalog']).sort()
    #write data to json file
    writeFile(all_apps_data, "apps.json")
    writeFile(filtered_apps_data, "filtered_apps.json") 


def scrapeURL(CSVrecord):
    app_name = CSVrecord["App Name"]
    SCRAPE_URL = CSVrecord["App Store URL"]
        
    # Only scrape United States pages (ones with /us/ in the URL).
    if(not SCRAPE_URL.find("/us/")):
        print ("Please enter only US urls")
        sys.exit(1)
            
    try:
        r = requests.get(SCRAPE_URL)
        r.raise_for_status()
        data = r.text
    except requests.exceptions.RequestException as err:
        print (err)
        sys.exit(1)
    
    soup = BeautifulSoup(data, 'lxml')
        
    divTags = soup.find_all("div", class_="information-list__item l-row")
        
    # get appid
    start = '"https://itunes.apple.com/us/app/id"'
    app_id = SCRAPE_URL[SCRAPE_URL.find(start) + len(start):]
        
    appData = {
                "languages": [],
                "app_identifier": app_id,
                "name": app_name,
                "minimum_version": ""
                }
    placeHolder = {}
    props = {"Compatibility", "Languages"}
    
    for divTag in divTags:
        dtTag = divTag.find("dt")
        dtTag_string = dtTag.string
        
        if(dtTag_string in props):
            ddTag = divTag.find("dd")
            if(ddTag.get('aria-label')):
                ddTag_string = ddTag.get('aria-label')
            else:
                ddTag_string = ddTag.string
                                    
            placeHolder[dtTag_string] = ddTag_string
    # process the data for Languages and compatability attributes.
    processData(placeHolder, appData)
    return appData
    

def filterApps(appData):   
    '''
    filters data for Filtered_apps.json such that:
    Filtered_apps.json should be a JSON dictionary, with keys:
    apps_in_spanish_and_tagalog - array of numbers, 
    sorted ascending - App identifiers of all apps that are available
    in both Spanish and Tagalog
    apps_with_insta_in_name - array of numbers, sorted ascending - 
    App identifiers of all apps apps that have “insta” 
    in the name (case insensitive)
    '''  
    #FILTER featured app data
       
    if re.search('insta', appData['name'], re.IGNORECASE):
        filtered_apps_data['apps_with_insta_in_name'].append(appData['app_identifier'])
            
    if("Spanish" in appData["languages"] and "Tagalog" in appData["languages"]):
        filtered_apps_data['apps_in_spanish_and_tagalog'].append(appData['app_identifier'])        
    

def processData(placeHolder, appData):
    ''' 
    Extract only the required fields minimum_version and Languages
    '''
    #process IOS version
    start = 'Requires iOS '
    end = ' or later.'
    s = placeHolder['Compatibility']
    placeHolder['Compatibility'] = (s[s.find(start)+len(start):s.rfind(end)])
    
    #process Languages
    placeHolder['Languages'] = placeHolder['Languages'].split(", ")
        
    #consolidate data
    appData["languages"] = placeHolder['Languages']
    appData["minimum_version"] = placeHolder['Compatibility']  
   

def writeFile(data, fileName):
    ''' 
    Write input dict data to a JSON file
    '''
    jsonData = json.dumps(data, indent=2)
    try:
        f = open(fileName, "w")
        f.write(jsonData)
        f.close()
    except IOError as e:
        print ("I/O error {}: {}".format(e.errno, e.strerror))   


def readFile(input_csv_name):
    ''' 
    Read input CSV file and return list of csvdict rows
    '''
    rows = []
    try:
        with open(input_csv_name) as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                rows.append(row)
    except IOError as e:
        print ("I/O error {}: {}".format(e.errno, e.strerror))
            
    return rows
    
if(__name__ == '__main__'):
    main()

