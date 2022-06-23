# Sector Knowledge Sharing (SKS) Server

## Project Overview
This repository is part of the Sectork Knowledge Sharing (SKS) Project. For a detailed overview of this project, please go to our [SKS Hub Roadmap](https://github.com/orgs/ajah/projects/4). The Roadmap has a more detailed readme, in addition to showing the upcoming updates, features and the known bugs/issues of the SKS Hub.

You can also find the SKS Hub interface code in this [repository](https://github.com/ajah/sks-interface) and the data from the hub in this [repository](https://github.com/ajah/skshub-data)


> Note: This documentation is not yet complete

This API reads the underlying CSV to serve data to the NPK Interface.

## Starting the Server

`./startup.sh`

## Data Processing

Navigate to the 'processing' directory (`cd sks-project/sks-backend/processing`) and run:

`python process_entities.py`

This will output to the data folder (sks-project/sks-backend/data):

- sks*entities_public*[timestamp].csv: Which becomes the file publicly available for download at [github.com/ajah/skshub-data]()
- sks*entities_interface*[timestamp].csv: Which becomes the file uploaded to Postgres & Elasticsearch for use by the front-end interface (this is the same as the public version except it contains text scraped from entity websites to enhance search results)
- sks*entities_merge*[timestamp].csv: For use in the next step in processing activities, which will append the ent_npk_id for applicable actitives

Next, run:

`python process_activities.py`

This will output an sks*activities*[timestamp] to the data folder and leverages the 'merge' version of the entities CSV above.

sks*activities*[timestamp].csv & sks*entities_interface*[timestamp].csv will be used by the Postgres and elasticsearch Handlers, which search the data folder for CSVs. For this reason, only keep **one** version of all CSVs in the data/processed directory at all times.

## Elasticsearch

### Search

When running locally, searches can be done using this URL syntax:

`http://localhost:5000/search?q=accessibility$filter=activity,entity`
