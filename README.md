# Sector Knowledge Sharing (SKS) Server

## Project Overview

This repository is part of the Sectork Knowledge Sharing (SKS) Project. For a detailed overview of this project, please go to our [SKS Hub Roadmap](https://github.com/orgs/ajah/projects/4). The Roadmap has a more detailed readme, in addition to showing the upcoming updates, features and the known bugs/issues of the SKS Hub.

You can also find the SKS Hub interface code in this [repository](https://github.com/ajah/sks-interface) and the data from the hub in this [repository](https://github.com/ajah/skshub-data)

## Docs Directory

- [Application Setup](#application-setup)
- [Modifying Custom Filters](#custom-filters)
- [Processing docs](#processing)
- [Web scraper docs](#web-scraper)

## Application Setup

### Requirements:

This application depends on a remote-hosted database and Elasticsearch instance.

During setup, take note of the following variables to export to the app in the next step:

- Remote-hosted database such as PostgreSQL
  - Database name
  - Database host address
  - Database port
  - Database user
  - Database password
- Remote-hosted Elasticsearch instance
  - ES Username
  - ES Password

### Steps

- Clone the Github repository
- Create a virtual environment for the repo
- Export the following variables:
  - FLASK_APP=./sks-backend/index.py
  - FLASK_ENV=development
  - DATABASE=[Database name]
  - HOST=[Database host address]
  - USER=[Database user
  - PASSWORD=[Database password]
  - DB_PORT=[Database port]
  - ES_USERNAME=[ES Username]
  - ES_PASSWORD=[ES Password]
- Test the application is running with:
  - flask run -h 0.0.0.0

Once the application is running, it acts as a a REST API to serve the data (once processed) to an interface.

## Custom Filters

Terms for the interface’s custom filters for the interface can be defined in sks-backend/controllers/custom_filters.py.

- Admins with access to the code can define inclusions and exclusions in dictionary format.
  - You can change or add new terms up to 5 terms per inclusion/exclusion
  - These will be treated as phrases by the search engine
- These will work immediately in the ES search for the filters already defined (efc_sustainability, efc_climate%20change, - efc_climate%20education) but new filters require front-end work in order to appear on the interface
- The results will contain at least one of the inclusions in either the visible fields accessible on the interface, or they may be hidden in the website text field not currently displayed there; and none of the exclusions

Usage:

- In a code editor or the Github interface, change the text within the square brackets to reflect the terms you’d like included and excluded by these filters

## Processing

- Before proceeding, ensure the main raw CSV is located at: sks-backend/data/raw/full_proactive_disclosure_dataset.csv
- Navigate to the processed/ directory
- Ensure you process the entities before the activites, as the process_activities.py script depends on the output of `process_entities.py`

### Processing Activities

Usage:

- Run: `python process_activities.py`
- Check the resulting CSV is exported to data/processed

### Processing Entities

The process_entities.py script outputs the following CSVs:

- \_interface: Renders a CSV containing all data plus features required for the functioning of the SKS hub interface (contains - the most information)
- \_public: Renders a CSV hosted on Github (skshub-data) for easy download, does not contain additional features for interface
- \_merge: Minimal CSV containing entity ids to merge wit the rpocess_activiates
- \_es: Elasticsearch-specific output for uploading to elasticstearch in following step

Usage:

- Run: `python process_entities.py`
- Check the resulting CSVs is exported to data/processed
- Run tests
- Check the output passes data tests by running this from the processing/ directory:
  - `pytest tests`

## Elasticsearch

### Search

When running locally, searches can be done using this URL syntax:

`http://localhost:5000/search?q=accessibility$filter=activity,entity`

## Web scraper

The web scraper takes in a CSV of entity domains and scrapes all the body text from their homepage. This information is then used to enhance search results on the application, but isn't current visible on the interface.

Usage:

- Ensure the "web_scraper_urls.csv" is housed in the webscraper/data folder, or add one by making a CSV from one of the `process_entities.py` outputs containing just the "BN" and "website" columns.
- Run `scrape.py`

The results will be saved to the webscraper/data folder as "output.csv".
