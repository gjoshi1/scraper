
Scraping
========
-Scrape iTunes public pages, such as this: https://itunes.apple.com/us/app/id1261357853.
-Data need to scraped is described in the Output section below.
-Only scrape United States pages (ones with /us/ in the URL).

Output
======
Given the input CSV of apps, write two JSON files, apps.json and filtered_apps.json, to the same directory as scraper.py

apps.json should be a JSON array, with array elements in the same order as the CSV and each array element containing keys:
name - string - The name of the app
app_identifier - number - The App Store’s identifier of the app (eg. 1261357853 for Fortnite)
minimum_ios_version - string - The minimum iOS version required to run the app
languages - array of strings, sorted alphabetically - All of the languages that the app supports

filtered_apps.json (example here) should be a JSON dictionary, with keys:
apps_in_spanish_and_tagalog - array of numbers, sorted ascending - App identifiers of all apps that are available in both Spanish and Tagalog
apps_with_insta_in_name - array of numbers, sorted ascending - App identifiers of all apps apps that have “insta” in the name (case insensitive)

Execute Code
============
python scraper.py input_csv_path 
