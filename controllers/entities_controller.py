import pandas as pd
import json 
import traceback

from .postgres_controller import read_table

def get_entity_postgres(_id):
  try:
    info = read_table(_id=_id, type='entities', identifier='ent_sks_id')
    data = json.dumps(info)
    return data 
  except:
    traceback.print_exc()

def get_test_entity():
  entity = {
    "BN": "106693120RR0001",
    "Legal Name": "ALBERTA DEBATE AND SPEECH ASSOCIATION",
    "Designation Type": "Charitable Organization",
    "City": "Airdrie",
    "Province Name": "Alberta",
    "Country Name": "Canada",
    "Postal Code": "T4B2Y3",
    "All Program Names": "Public education, other study programs",
    "FPE": "2019-01-31",
    "Contact URL": "www.albertadebate.com",
    "Total Revenue": 98362,
    "Total Employees": 1,
    "NKP_id": 12345,
    "Regulating Authority": "CRA",
    "Revenue range currency": "CAD"
  }

  return entity




