# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

import requests 
import urllib
import time
from dataiku.customrecipe import *
import sys

#disable InsecureRequestWarning.
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

print ('## Running Plugin v0.5.0 ##')

input_name = get_input_names_for_role('input')[0]

# Recipe out

output_ = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_)

schema = [
             {'name':'matchAddress','type':'string'}
            ,{'name':'storeStreeNumber','type':'string'}
            ,{'name':'storeStreet','type':'string'}
            ,{'name':'storeCity','type':'string'}
            ,{'name':'storeState','type':'string'}
            ,{'name':'storeCounty','type':'string'}
            ,{'name':'storeCountry','type':'string'}
            ,{'name':'storePostalCode','type':'string'}
            ,{'name':'storeLatitude','type':'string'}
            ,{'name':'storeLongitude','type':'string'} 
            ,{'name':'storeMatchLevel','type':'string'}
            ,{'name':'storeMatchQualityCity','type':'string'}
            ,{'name':'storeMatchQualityHouseNumber','type':'string'}
            ,{'name':'storeMatchQualityPostalCode','type':'string'}
            ,{'name':'storeMatchQualityState','type':'string'}
            ,{'name':'storeMatchQualityStreet','type':'string'}
        ]


P_PAUSE = int(get_recipe_config()['param_api_throttle'])
P_SEARCH_ADDRESS = get_recipe_config()['p_search_address']
P_API_KEY = get_recipe_config()['p_api_key']

P_CALL_COUNT = 0
    
P_BATCH_SIZE_UNIT = int(get_recipe_config()['param_batch_size'])
if P_BATCH_SIZE_UNIT is None:
    P_BATCH_SIZE_UNIT = 50000
    
strategy = get_recipe_config()['param_strategy']

if get_recipe_config().get('p_id_column', None) is not None and get_recipe_config().get('p_id_column', None) <>'':
    use_column_id=True
    id_column = get_recipe_config().get('p_id_column', None)
    id_as_int = get_recipe_config().get('param_id_as_int', None)
    
    if id_as_int:
        schema.append({'name':id_column,'type':'int'})
    else:
        schema.append({'name':id_column,'type':'string'})
else:
    use_column_id=False

output_dataset.write_schema(schema)

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

b=-1 
with output_dataset.get_writer() as writer:
    for df in dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT ):

        b = b +1
        n_b = b * P_BATCH_SIZE_UNIT 


        df = df[abs(df[P_SEARCH_ADDRESS]>0)]

        if strategy =='make_unique':
            dfu = df.groupby([P_SEARCH_ADDRESS]).count().reset_index()
        else:
            dfu = df.copy()


        n__ = -1
        for v in dfu.to_dict('records'):

            n__ = n__ + 1
            n_record = n_b + n__

            searchtext = v[P_SEARCH_ADDRESS]
            
            if use_column_id:
                id_ = v[id_column]

            
            print '%s - processing: (%s)' % (n_record, searchtext)
            
            # Encode parameters
            params = urllib.urlencode(
                {'searchtext': searchtext,
                 'apiKey': P_API_KEY,
                 }
            )
            # Contruct request URL
            url = 'https://geocoder.ls.hereapi.com/6.2/geocode.json?' + params            
            print(url)
            for P_CALL_COUNT in range(0, 4):
                call = requests.get(url, verify=False)
                if call.status_code == 200:
                    geoData = call.json()
                    try:
                        flat_data = flatten_json(geoData)
                        # print(flat_data)
                        d = {}

                        d['matchAddress'] = flat_data['Response_View_0_Result_0_Location_Address_Label']
                        d['storeStreeNumber'] = flat_data['Response_View_0_Result_0_Location_Address_HouseNumber']
                        d['storeStreet'] = flat_data['Response_View_0_Result_0_Location_Address_Street']
                        d['storeCity'] = flat_data['Response_View_0_Result_0_Location_Address_City']
                        d['storeState'] = flat_data['Response_View_0_Result_0_Location_Address_State']
                        d['storeCounty'] = flat_data['Response_View_0_Result_0_Location_Address_County']
                        d['storeCountry'] = flat_data['Response_View_0_Result_0_Location_Address_Country']
                        d['storePostalCode'] = flat_data['Response_View_0_Result_0_Location_Address_PostalCode']
                        d['storeLatitude'] = flat_data['Response_View_0_Result_0_Location_NavigationPosition_0_Latitude']
                        d['storeLongitude'] = flat_data['Response_View_0_Result_0_Location_NavigationPosition_0_Longitude']
                        d['storeMatchLevel'] = flat_data[u'Response_View_0_Result_0_MatchLevel']
                        d['storeMatchQualityCity'] = flat_data['Response_View_0_Result_0_MatchQuality_City']
                        d['storeMatchQualityHouseNumber'] = flat_data['Response_View_0_Result_0_MatchQuality_HouseNumber']
                        d['storeMatchQualityPostalCode'] = flat_data['Response_View_0_Result_0_MatchQuality_PostalCode']
                        d['storeMatchQualityState'] = flat_data['Response_View_0_Result_0_MatchQuality_State']
                        d['storeMatchQualityStreet'] = flat_data['Response_View_0_Result_0_MatchQuality_Street_0']
                        
                        # print(d)

                        col_list_ = ['matchAddress',
                                     'storeStreeNumber',
                                     'storeStreet',
                                     'storeCity',
                                     'storeState',
                                     'storeCounty',
                                     'storeCountry',
                                     'state_code',
                                     'storePostalCode',
                                     'storeLatitude',
                                     'storeLongitude',
                                     'storeMatchLevel',
                                     'storeMatchQualityCity',
                                     'storeMatchQualityHouseNumber',
                                     'storeMatchQualityPostalCode',
                                     'storeMatchQualityState',
                                     'storeMatchQualityStreet'
                                     ]

                        if use_column_id is True:
                            if id_as_int:
                                d[id_column] = int(id_)
                            else:
                                d[id_column] = id_

                        writer.write_row_dict(d)
                        break
                    except:
                        print 'Unable to find these coordinates in the Here API after 4 tries: Record #:%s, url:%s' % (
                            n_record, url)
                else:
                    print 'Failed. API status: %s, retrying in P_CALL_COUNT*0.500 seconds.' % (call.status_code)
                    time.sleep(P_CALL_COUNT*0.500)
            # call = requests.get(url, verify=False)
            time.sleep(P_PAUSE)
