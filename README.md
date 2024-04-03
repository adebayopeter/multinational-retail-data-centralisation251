# AiCore Multinational Retail Data Centralisation

Retail Data Centralisation project 

## Table of Contents

1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Description

This project was initiated to resolve the challenges of scattered sales data spread across multiple data sources. This is data-centric approach aims to centralize all sales data making it easily accessible and analyzable from one unified platform. This entails fetching data from various data source such as API calls, json files, pdf, csv and AWS database. The datas are cleaned and pushed to a centralised on prem postgres database. I have learnt how to connect to different data source, clean up data and push to the database.

## Installation

To install the Retail Data Centralisation project, you can clone the repository from GitHub:

```bash
git clone https://github.com/adebayopeter/multinational-retail-data-centralisation251.git
```
Navigate to the project directory and install the required dependencies:
```
cd multinational-retail-data-centralisation251
pip install -r requirements.txt
```
Create a folder csv that would run all csv file generated when you run main.py. Please note that all Task can be run on main.py all you just have to do is to uncomment the block of code on main.py and run it. E.g. Unblock block codes for Milestone 2: Task 7 on main.py to solve Task 7 problems. Comment it back when you are done and uncomment any other block you which to continue with.

Edit `db_creds_example.yaml` and `db_creds_local_example.yaml` by supplying your database credentials and also rename them by removing `_example`.

## File Structure
The project directory is structured as follows:

```
📦 multinational-retail-data-centralisation251
├─ data_cleaning.py
├─ data_extraction.py
├─ database_utils.py
├─ db_creds_example.yaml
├─ db_creds_local_example.yaml
├─ main.py
├─ README.md
├─ requirements.txt
└─ sql_script.sql
```
## License
This project is licensed under [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)