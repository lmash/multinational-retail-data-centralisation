# Multinational Retail Data Centralisation
This is a multinational company which has sales data spread across many different sources. 
This means the data isn't easily accessible and therefore analysis of the data is difficult. 
Help the company utilize their data better. 

## Table of Contents
* [Project description](#project-description)
* [Project aim](#project-aim)
* [What I've learned](#what-ive-learned)
* [Tech Stack](#tech-stack)
* [Installation](#installation)
* [Usage](#usage)
* [File structure](#file-structure)
* [Run tests](#run-tests)
* [Licence](#license)

#### Project description
This project creates a central location for the sales data of the company. 
The project collects, cleans and loads the data into a database. It then creates a star-based database schema
with multiple dimension tables and a single fact table. The sales data is queried to answer 
business questions.

#### Project aim
Practice and utilize skills learned in Python, Pandas and Sql in a valid business case. This project ties all the 
learning together and therefore builds ones confidence by being able to deliver an end to end useful product.

#### What I've learned
  - The power of using pandas and notebooks for investigation, and then with each problem solved encompassing the solution into it's own function.
  - Use dataframe masks more, they increase code readability.
  - How to diagnose and resolve SettingWithCopy issues.
  - Adding test coverage is invaluable! Cannot refactor code without them.
  - Patterns emerge after doing the same task a few times, efficiencies can then be found.
  - Reading in pdf's to pandas results in duplicate indexes being created
  - Requests issues are tricky to debug
  - How to create a star-based schema
  - Increased confidence with SQL queries and database schema manipulation

### Tech Stack
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)

## Installation
Pre-requisite: 
* Conda/miniconda installed, 

1. Clone the repo

```sh
git clone https://github.com/lmash/multinational-retail-data-centralisation.git
```

2. Change to the multinational-retail-data-centralisation folder

```sh
cd multinational-retail-data-centralisation
```

3. Create the conda env and install required packages

```shell
conda env create -f environment.yml
```

4. .env_bkp populated with value for x-api-key. Renamed .env
5. db_creds.bkp file RDS_ params populated with values for connecting to source database. Renamed db_creds.bkp
6. db_creds_target.bkp file RDS_ params populated with values for connecting to target database. Renamed db_creds_target.bkp

## Usage

Run the below from the command line to extract, clean and load the data, and then create the star-based 
schema. Queries can be found in sql_queries.sql

```sh
python __main__.py
./run_sql.sh
```
### File structure
```
├── README.md
├── .env_bkp
├── __main__.py
├── config
│   ├── db_creds.bkp
│   └── db_creds_target.bkp
├── config.py
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── environment.yml
├── investigate.ipynb
├── run_sql.sh
├── sql_queries.sql
├── sql_scripts
│   ├── 0_drop_tables.sql
│   ├── 1_orders_table_cast_columns.sql
│   ├── 2_dim_users_table_cast_columns.sql
│   ├── 3_dim_store_details_cast_columns.sql
│   ├── 4_update_dim_products.sql
│   ├── 5_dim_products_cast_columns.sql
│   ├── 6_dim_date_times_cast_columns.sql
│   ├── 7_dim_card_details_cast_columns.sql
│   ├── 8_create_primary_keys.sql
│   └── 9_create_foreign_keys_on_orders_table.sql
└── test_data_cleaning.py
```

## Run tests

```sh
cd <multinational-retail-data-centralisation>
pytest --verbose
```

### License
Licensed under the [GPL-3.0](https://github.com/lmash/multinational-retail-data-centralisation//blob/main/LICENSE) license.
