"""Author             :AMARNADH G
   Date last modified :2020-12-12
   Description        :Pulls Google Anlytics data with pagination and unsampled data
   Comments           :Dimentions, Metrics and DateRanges are dynamic in nature in which daterange is parameterised""" 

###GOOGLE ANALYICS V4

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime,timedelta
import io

todayStr=datetime.today().strftime('%Y-%m-%d')
YstrdyInt = datetime.today()-timedelta(days=1)
YstrdyStr=datetime.strftime(YstrdyInt,'%Y-%m-%d')


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'C:\\Users\GUNAKALA\Desktop\Python\ga-data-extract-285512-eae7bcedecf2.json'
VIEW_ID = '166677761'
PAGESIZE = 100000


def initialize_analyticsreporting():

  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics




def get_PT(response):
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    pageToken=report.get('nextPageToken', None)
    print(str(pageToken)+' at 43')
  return pageToken

  
def get_report(analytics,pageToken='unknown'):

  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': PAGESIZE,
          "samplingLevel": "LARGE",
          'pageToken':pageToken,
          'dateRanges': [{'startDate':'2020-09-01', 'endDate':'2020-09-01'}],
          'metrics': [{"expression":"ga:sessions"}, {"expression":"ga:avgPageLoadTime"}, {"expression":"ga:pageviewsPerSession"}]       ,
          'dimensions': [{"name":"ga:deviceCategory"}, {"name":"ga:browser"}, {"name":"ga:date"}, {"name":"ga:pagePath"}]   
        }]
      }
  ).execute()


def print_response(response):

  f = io.open('Sessions'+ todayStr +'.txt', 'a+',encoding="utf-8")
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    #pageToken=report.get('nextPageToken', None)

    #rint(pageToken)
    print(columnHeader)
      #writing dimention header
    for D_header in dimensionHeaders:
        f.write(str.capitalize(str.replace(D_header,'ga:','')) +'|')
        #print(D_header)

    for M_header in list( columnHeader['metricHeader']['metricHeaderEntries']):
      f.write(str.capitalize(str.replace(M_header['name'],'ga:','')) +'|')


    f.write('\n')
    
    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      Metrics = row.get('metrics', [])
     
    #writing dimention header row data  
      for  dimension in (dimensions):
        f.write(dimension+'|')
    #writing metric header
      for i, values in enumerate(Metrics):
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          f.write( value+'|')
      f.write('\n')     
  f.close()


def main():
  analytics = initialize_analyticsreporting()
  response = get_report(analytics)

  pageToken=get_PT(response)
  
  print(str(pageToken)+' at 108')

  print_response(response)
 
  while pageToken:
    print('inside while '+ str(pageToken))
    analytics = initialize_analyticsreporting()
    response = get_report(analytics,pageToken)
    pageToken=get_PT(response)
    print_response(response)
    print(str(pageToken)+' at 118')
    

if __name__ == '__main__':
  main()
