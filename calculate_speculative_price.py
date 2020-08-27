import requests
import json
import pyodbc
import pandas as pd
from pprint import pprint

#### GET DATA FOR SIMULATION
def get_data_frame():
  cnxn = pyodbc.connect('Driver={XX};'
          'Server=XX;'
          'Database=XX;'
          'Trusted_Connection=XX;')
  cursor = cnxn.cursor()
  sql = 'select * from table'
  df = pd.read_sql(sql,cnxn)
  cnxn.close()
  df_json = df.to_json(orient='records')
  data = json.loads(df_json)
  return data


def make_specinput(optioncombination,prop,specvalue):
  spec_input = {
    "chainName": "SalePriceOptionCombination",
    "optionCombinationId": optioncombination,
    "propertyName": prop,
    "speculativeValue": specvalue
  }
  return spec_input

def make_delphi_request(sku,bclgid,specinput_list):
  request = {
    "sku": sku,
    "bclgId": bclgid,
    "speculativeInputs": specinput_list
  }
  return request

def make_post_payload(request_list):
  response = {
    "chain": "SalePriceProduct",
    "verboseResponse": True, #Set to False if you do not want the RequestObject
    "requests": request_list
  }
  return response

def main():
  delphi_data_frame = get_data_frame()
  #### keys are (BCLGID,SKU), values are lists of spec input object
  request_mapping = {}
  for row in delphi_data_frame:
    prop = "Elasticity"
    bclgid = row["BclgID"]
    sku = row["PrSKU"]
    ocid = row["OptionCombinationId"]
    elasticity = row["Test_Elasticity"]
    map_key = (bclgid,sku)
    spec_input = make_specinput(ocid,prop,elasticity)
    if map_key in request_mapping:
      request_mapping[map_key].append(spec_input)
    else:
      request_mapping[map_key] = [spec_input] 

  request_list = []    
  for row in request_mapping.items():
    bclgid_sku = row[0]
    bclgid = bclgid_sku[0]
    sku = bclgid_sku[1]
    spec_input_list = row[1]
    request = make_delphi_request(sku,bclgid,spec_input_list)    
    request_list.append(request) 

  payload = make_post_payload(request_list)  
  return payload

pprint(main())

#### MOCK AUTHENTICATION FOR QA SPEC DELPHI API
mock_url = 'XX'
headers = {'content-type': 'application/json'}
body = {
    "Issuer": "XX",
    "Scopes": ["XX",
      "XX",
      "XX",
      "XX"],
    "ClientId": "XX"
  }
delphi_url = "XX"


#### Get Mocked Auth Tocken
authorization = requests.post(mock_url, headers = headers, data = json.dumps(body))
auth_results = authorization.json()
auth_payload = auth_results["payload"]
auth_headers = {"Authorization": f"Bearer {auth_payload}", "Content-Type": "application/json"}
print(auth_headers)

#### Pass Auth Tocken / Simulation to Spec Delphi
delphi_reponses = requests.post(delphi_url, headers = auth_headers, json = main())


delphi_payload = delphi_json["payload"]
delphi_response = delphi_payload["responses"]
prices = delphi_response[0]
optioncomboprices = prices["optionCombinationResponses"]

for optioncomboprice in optioncomboprices:
	request_obj = optioncomboprice["optionCombinationRequest"]
	json_request_obj = json.loads(request_obj)
	current_price_obj = json_request_obj["CurrentPrice"]
	handler = optioncomboprice["handledBy"]
	OcId = optioncomboprice["optionCombinationId"]
	sale_price = optioncomboprice["price"]
	print(OcId,sale_price,handler)
