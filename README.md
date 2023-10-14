# Multinational Retail Data Centralisation
T.B.C.
Help a multinational company utilize their data better. This project creates a central location for the sales data of the company. 
The data is currently spread across multiple sources in varying formats. The project collects, 
cleans and loads the data into a single database which can then be analysed.

### Table of Contents
T.B.C.

### Project description
T.B.C.
A description of the project: what it does, the aim of the project, and what you learned
  - The power of using pandas and notebooks for investigation, and then with each problem solved encompassing the solution into it's own function.
  - Use dataframe masks more, they increase code readability.
  - How to diagnose and resolve SettingWithCopy issues.
  - Adding test coverage was invaluable! Cannot refactor code without them.
  - Patterns emerge after doing the same task a few times, efficiencies can then be found.
  - Reading in pdf's to pandas results in duplicate indexes being created
  - Requests issues are tricky to debug

### Installation
Pre-requisite: Conda/miniconda installed

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

### Usage

Run the following from the command line

```sh
python __main__.py
```

## Tech Stack
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)

### File structure
T.B.C.
```
├── README.md
├── __main__.py
├── config
│   ├── db_creds.yaml
│   └── db_creds_target.yaml
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── investigate.ipynb
└── test_data_cleaning.py
```

### Description of files
T.B.C.
Non-Python files:

| filename         | description                                             |
| ---------------- | ------------------------------------------------------- |
| README.md        | Text file (markdown format) description of the project. |
| environment.yaml | Text file (yaml format) Conda environment file          |

Python modules:

| filename              | description                         |
|-----------------------|-------------------------------------|
| __main__.py           | Run the pipeline                    |
| config.py             | t.b.c.                              |
| data_cleaning.py      | t.b.c.                              |
| data_extraction.py    | t.b.c.                              |
| database_utils.py     | t.b.c.                              |
| test_data_cleaning.py | Unit tests for data cleaning module |

### Run tests

```sh
cd <multinational-retail-data-centralisation>
pytest --verbose
```

### License
T.B.C.
Licensed under the [GPL-3.0](https://github.com/lmash/hangman/blob/main/LICENSE) license.
