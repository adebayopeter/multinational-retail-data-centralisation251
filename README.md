# AiCore Multinational Retail Data Centralisation

Retail Data Centralisation project 

## Table of Contents

1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Description

This project was initiated to resolve the challenges of scattered sales data spread across multiple data sources. This is data-centric approach aims to centralize all sales data making it easily accessible and analyzable from one unified platform. This entails fetching data from various data source such as API calls, json files, pdf, csv and AWS database. The datas are cleaned and pushed to a centralised on prem postgres database.

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

## File Structure
The project directory is structured as follows:
```
📦 multinational-retail-data-centralisation251
├─ data_cleaning.py
├─ data_extraction.py
├─ database_utils.py
├─ main.py
├─ README.md
├─ requirements.txt
└─ sql_script.sql
```

## License
This project is licensed under [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)