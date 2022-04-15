import json
import traceback
from .postgres_controller import read_table

def get_activity_postgres(_id):
  try:
    info = read_table(_id, type='activities',identifier = 'act_sks_id')
    data = json.dumps(info)
    return data 
  except:
    traceback.print_exc()

def get_5_most_recent_activities_per_entity(ent_sks_id):
  try:
    info = read_table(ent_sks_id, type='activities',identifier = 'ent_sks_id',limit=5)
    data = json.dumps(info)
    return data 
  except:
    traceback.print_exc()


def get_activities_per_entity(ent_sks_id):
  try:
    info = read_table(ent_sks_id, type='activities',identifier = 'ent_sks_id', limit=200)
    data = json.dumps(info)
    return data 
  except:
    print("An error occurred")


def get_test_activity():
  activity  = {
    "npk_id": 12349,
    "source_id": "05-2019-2020-Q4- 16819005",
    "source_authority": "Proactive Disclosure - Grants and Contributions",
    "source_url": "https://open.canada.ca/data/dataset/432527ab-7aac-45b5-81d6-7597107a7013/resource/1d15a62f-5656-49ad-8c88-f40ce689d831/download/grants.csv",
    "grant_title": "The Future of Work and Disability",
    "funding_amount": 99984,
    "funding_type": "Amount Applied For",
    "funder": "casdo-ocena",
    "recipient_organization": "Ontario College of Art and Design University",
    "recipient_id": 0,
    "grant_description": "The program Advancing Accessibility Standards Research (The Program) funds the project. The Program funds research projects. The projects try to find, take away and stop obstacles to accessibility. This research will help to guide accessibility standards in the future.",
    "funder_id": 0,
    "grant_region": "ON",
    "grant_municipality": "Toronto",
    "date": "2020-03-27",
    "date_type": "Agreement Start",
    "end_date": "2020-10-16",
    "end_date_type": "Agreement End",
    "expected_results": "Funded Research projects help build accessibility standards. The research also help set the priorities for the development of standards.",
    "actual_results": 0,
    "program_name": "Advancing Accessibility Standards Research",
    "org_redirect" :"http://localhost:5000/entities/32290"
  }

  return activity

