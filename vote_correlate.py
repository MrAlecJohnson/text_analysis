"""Gets data to analyse whether pageRating votes correlate with page length
Finds unique pageviews in 2019, wordcount and template of currently live pages"""

import os
import string
import requests
import pandas as pd

from bs4 import BeautifulSoup 
from google.cloud import bigquery
from apiclient.discovery import build
from multiprocessing.dummy import Pool 
from oauth2client.service_account import ServiceAccountCredentials

# Authenticate for BigQuery to get list of live pages
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/alec/Python/KEYS/backlogger_bq.json'

# Authenticate for and Google Analytics to get votes and views
ANALYTICS_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/Users/alec/Python/KEYS/backlogger_bq.json'

ga_creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, ANALYTICS_SCOPES)

#%%
# Set up functions for getting data from Google Analytics
def get_analytics(analytics, query):
    """Standard Google Analytics API function
    Pass it the standard analytics object and a reportRequests query
    """
    result = analytics.reports().batchGet(
        body={'reportRequests': query}
        ).execute()
    return list_response(result)

def list_response(response):
    """ A standard Google Analytics function 
    Use on results of query functions to turn them into DataFrames
    """
    lst = []
    # Structures the data into a table
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

    for row in rows:
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
            dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
            for metric, value in zip(metricHeaders, values.get('values')):
                #set int as int, float a float
                if '.' in value or '.' in value:
                    dict[metric.get('name')] = float(value)
                else:
                    dict[metric.get('name')] = int(value)

        lst.append(dict)
    
    return pd.DataFrame(lst)

#%%
# Make the Google Analytics data requests
START = '2019-01-01'
END = '2019-12-31'
VIEW_ID = 'ga:93356290' # just public site
PAGES = '^/(benefits|consumer|debt-and-money|family|health|housing|immigration|law-and-courts|work)/'

views_query = [ # Gets advice pages and their unique pageviews
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': START, 'endDate': END}],
          'dimensions': [{'name': 'ga:pagePath'}, 
                         {'name': 'ga:dimension2'}],          
          'metrics': [{'expression': 'ga:uniquePageviews'}, 
                     ],
          'dimensionFilterClauses': [{'operator': 'AND', 'filters': 
              [
          {
                  'dimensionName': 'ga:pagePath',
                  'operator': 'REGEXP',
                  'expressions': PAGES
                  },
          ]
            }],
          'orderBys': [{'fieldName': 'ga:pagePath', 'sortOrder': 'ASCENDING'}],
          'pageSize': 10000
        }]

votes_query = [ # Gets advice pages and their vote counts
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': START, 'endDate': END}],
          'dimensions': [{'name': 'ga:eventAction'},
                         {'name': 'ga:eventLabel'}],          
          'metrics': [{'expression': 'ga:uniqueEvents'}, 
                     ],
          'dimensionFilterClauses': [{'operator': 'AND', 'filters': 
              [
          {
                  'dimensionName': 'ga:eventAction',
                  'operator': 'REGEXP',
                  'expressions': PAGES
                  },
          {
                  'dimensionName': 'ga:eventCategory',
                  'operator': 'EXACT',
                  'expressions': 'pageRating'
                  }
          
          ]
            }],
          'orderBys': [{'fieldName': 'ga:eventAction', 'sortOrder': 'ASCENDING'}],
          'pageSize': 10000
        }]

analytics = build('analyticsreporting', 'v4', credentials = ga_creds)
views = get_analytics(analytics, views_query)
votes = get_analytics(analytics, votes_query)

joined = views.merge(votes, "inner", 
                    left_on = "ga:pagePath", 
                    right_on = "ga:eventAction") \
                .drop("ga:eventAction", axis = 1)

stats = joined.pivot_table('ga:uniqueEvents', 
                           ['ga:pagePath', 
                            'ga:dimension2', 
                            'ga:uniquePageviews'], 
                           'ga:eventLabel').reset_index()

stats.rename(index = str, columns = {
    "ga:pagePath": "Page",
    "ga:dimension2": "Template",
    "ga:uniquePageviews": "UniquePageviews",
    "yes": "YesVotes",
    "no": "NoVotes"}, 
    inplace = True)

stats[['YesVotes', 'NoVotes']].fillna(0, inplace = True)
stats['TotalVotes'] = stats['YesVotes'] + stats['NoVotes']
stats['Positive%'] = stats['YesVotes']/stats['TotalVotes']

#%%
# Get data from BigQuery and merge to remove pages that have expired
def get_current_data(client):
    """pagesreport_view is always the latest data in the table 
    This gets the relevant data from the view
    """
    query = client.query("""
        SELECT DISTINCT
          Path
        FROM `pagesreport.pagesreport_view`
        WHERE
          REGEXP_CONTAINS(Path, "^/(benefits|consumer|debt-and-money|family|health|housing|immigration|law-and-courts|work)/")
          AND StopPublish IS NULL
          AND Shortcut NOT LIKE 'Shortcut%'
          """)
        
    results = query.result()  
    return results.to_dataframe()

client = bigquery.Client()
current = get_current_data(client)

df = stats[stats['TotalVotes'] >= 100] \
        .merge(current, "inner", left_on = "Page", right_on = "Path") \
        .drop("Path", axis = 1)

#%%
# Scrape the pages to get their word counts
# Use multithreading so it doesn't take forever
punctuation = string.punctuation + '““””•…–—€’'

def word_count(url):
    """Go to page, find article content, 
    delete the punctuation and count the words.
    Return 0 if you don't get a proper response
    """
    response = requests.get('http://www.citizensadvice.org.uk' + url)
    if response.ok:
        soup = BeautifulSoup(response.text, 'lxml')
        content = soup.find(class_='articleContent').text
        stripped = content.translate(str.maketrans(' ', ' ', punctuation))
        return {'Page': url, 'WordCount': len(stripped.split())}
    else:
        print(url, response)
        return {'Page': url, 'WordCount': 0}

# Use the thread pool to map the word count function to the url list
pool = Pool(4) 
urls = df['Page'].tolist()
results = pd.DataFrame(pool.map(word_count, urls))

# Now merge the word counts back into the main dataframe and export it to csv
df = df.merge(results, "inner", on = "Page")
df.to_csv('pagedata.csv', index = 0)