# Multinational Retail Data Centralisation
T.B.C.
This is an implementation of the Hangman game, where the computer thinks of a word and the user tries to guess it.

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
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![NumPy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)
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

| filename              | description                          |
|-----------------------|--------------------------------------|
| __main__.py           | Run the pipeline                     |
| data_cleaning.py      | t.b.c.                               |
| data_extraction.py    | t.b.c.                               |
| database_utils.py     | t.b.c.                               |
| test_data_cleaning.py | Unit tests for data cleaning module  |

### Run tests

```sh
cd <multinational-retail-data-centralisation>
pytest --verbose
```

### License
T.B.C.
Licensed under the [GPL-3.0](https://github.com/lmash/hangman/blob/main/LICENSE) license.
