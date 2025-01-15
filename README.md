
# setting up

### Environment

a virtual environment can be created on the top level of this project, and all necessary dependancies installed from the pyproject.toml file. Enter the air_quality_monitoring directory to run subsequent steps.

its recommended to make sure pip is up to date with the latest version first to avoid issues.

```
>>> python -m pip install --upgrade pip
>>> python -m venv .venv
>>> .venv/scripts/activate
(.venv)>>> pip install -e .
(.venv)>>> cd air_quality_monitoring
```

### MySQL

a mysql server must be running on port 3306. This address is hard coded in the db connection details. If your mysql is running on a different port, change the port number using in `air_quality_monitoring\import.py` 

you can create this MySQL server using XAMPP (used my me), Laragon, a docker image or another method of your choice.

To create the model, execute the sql found in `air_quality_monitoring/pollution.sql`. Use the sql client of your choice. 

Then, the data can be imported in two steps
    1. Crop and clean the seed data by running `air_quality_monitoring/cropped.py`
    2. Import the data into the db by running `air_quality_monitoring/import.py`

```
(.venv)>>> python air_quality_monitoring/cropped.py
(.venv)>>> python air_quality_monitoring/import.py
```

### Quest db

a quest db image can be run using 
`docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 -v path/to/volume/mount:/var/lib/questdb questdb/questdb:8.2.1`

for my project, the directory i mount is `C:/users/daniel/documents/dmf/assignment/questdb_volume`.
the directory `questdb_volume` can be found inside this project.

then, the model and data can by running the quest_inquest script:

```
(.venv)>>> python .\air_quality_monitoring\quest_ingest.py
```




my docker command:
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 -v C:/users/daniel/documents/dmf/assignment/questdb_volume:/var/lib/questdb questdb/questdb:8.2.1
