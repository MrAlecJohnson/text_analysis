# Does number of votes inversely correlate with page length? 
# Is number of votes affected by how positive those votes are? 
# Any other correlations? 

import requests
import string
import re
import pandas as pd

from bs4 import BeautifulSoup 
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

def authenticate(key_file, service):
    """Function to authenticate Sheets or Analytics. 
    Pass it SHEETS_SCOPE or ANALYTICS_SCOPE
    """
    return ServiceAccountCredentials.from_json_keyfile_name(key_file, service)

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/alec/Python/KEYS/backlogger_bq.json'
ANALYTICS_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/Users/alec/Python/KEYS/backlogger_bq.json'

ga_creds = authenticate(KEY_FILE_LOCATION, ANALYTICS_SCOPES)

#%%

style_analytics = '%Y-%m-%d'
style_user = ''

START = '2019-01-01'
END = '2019-12-31'
VIEW_ID = 'ga:93356290' # just public site
PAGES = '^/(benefits|consumer|debt-and-money|family|health|housing|immigration|law-and-courts|work)/'

def list_response(response):
    """ A standard Google Analytics function 
    Rearranges results into nested lists
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

def get_views(creds):
    """Standard Google Analytics API function
    Gets the pages and their views
    """
    analytics = build('analyticsreporting', 'v4', credentials=creds)
    result = analytics.reports().batchGet(
        body={
        'reportRequests': [
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
      }
  ).execute()
    return list_response(result)

def get_votes(creds):
    """Standard Google Analytics API function
    Gets the event count for the pages
    """
    analytics = build('analyticsreporting', 'v4', credentials=creds)
    result = analytics.reports().batchGet(
        body={
        'reportRequests': [
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
      }
  ).execute()
    return list_response(result)

#%%
    
views = get_views(ga_creds)
votes = get_votes(ga_creds)

joined = views.merge(votes, "inner", 
                    left_on = "ga:pagePath", 
                    right_on = "ga:eventAction").drop("ga:eventAction", axis = 1)

stats = joined.pivot_table('ga:uniqueEvents', 
                           ['ga:pagePath', 'ga:dimension2', 'ga:uniquePageviews'], 
                           'ga:eventLabel').reset_index()
#%%
del views
del votes
del joined

stats.rename(index = str, columns = {
    "ga:pagePath": "Page",
    "ga:dimension2": "Template",
    "ga:uniquePageviews": "UniquePageviews",
    "yes": "YesVotes",
    "no": "NoVotes"}, 
    inplace = True)

stats['YesVotes'].fillna(0, inplace = True)
stats['NoVotes'].fillna(0, inplace = True)
stats['TotalVotes'] = stats['YesVotes'] + stats['NoVotes']
stats['Positive%'] = stats['YesVotes']/stats['TotalVotes']

df = stats[stats['TotalVotes'] >= 100]

#%%

test = '/consumer/holiday-cancellations-and-compensation/if-your-flights-delayed-or-cancelled/'
punctuation = string.punctuation + '““””•…–—€’'

def content(url):
    response = requests.get('http://www.citizensadvice.org.uk' + url)
    soup = BeautifulSoup(response.text, 'lxml')
    content = soup.find(class_='articleContent').text
    stripped = content.translate(str.maketrans(' ', ' ', punctuation))
    return stripped

def count_words_regex(text):
    return len(re.findall(r'\w+', text))

def count_words_split(text):
    return len(text.split())

# BENCHMARKING FUNCTION
    
import time

def speed(reps, func, **kwargs):
    """times a function in milliseconds, averaged over reps repetitions
    """
    start = time.perf_counter()
    # Run the selected function, using **kwargs to supply arguments
    for x in range(reps):
        result = func(**kwargs)
    end = time.perf_counter()
    # multiply by 1,000 to convert to milliseconds
    return ((end - start) * 1000) / reps

words = content(test)

print('regex:', speed(100, count_words_regex, text = words))
print('split:', speed(100, count_words_split, text = words))