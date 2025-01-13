import pandas as pd
from pathlib import Path

# from questdb.ingress import Sender
import requests
import os

import shapely as shp
import geopandas as gpd

# a lot of code in this file is duplicated from code in input.py.
# This is because i can't import from import (keyword reserved). but i didn't want to change the name of a submission file
# There are some changes, but most of the new code is in functions named with a "import_{tablename}_parquet" convention (line 155 and down)


def get_readings_data() -> pd.DataFrame:
    print("reading csv")
    file = Path("air_quality_monitoring\\data\\air_quality_cropped.csv")
    data = pd.read_csv(file)
    print("setting datatypes")
    data = data.astype(
        {
            "reading_id": "int32",
            "Date_Time": "datetime64[ns, UTC]",
            "NOx": "float32",
            "NO2": "float32",
            "NO": "float32",
            "PM10": "float32",
            "O3": "float32",
            "Temperature": "float32",
            "NVPM2_5": "float32",
            "PM2_5": "float32",
            "VPM2_5": "float32",
            "CO": "float32",
            "RH": "float32",
            "Pressure": "float32",
            "SO2": "float32",
        }
    )

    # before using the date time as a formatted string, then casting back to timestamp in the import statement (below),
    # I was getting nonsensical timestamps in the db, like the year 4035
    data["Date_Time"] = data["Date_Time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    data["Date_Time"] = data["Date_Time"].astype("string")

    print(data.head())
    return data


def get_constituencies_data() -> pd.DataFrame:
    print("reading csv")
    file = Path("air_quality_monitoring\\data\\constituencies.csv")
    data = pd.read_csv(file, names=["constituency_name", "constituency_id"])
    data = data[["constituency_id", "constituency_name"]]  # swap column over to be neat
    print("setting datatypes")
    data = data.astype(
        {
            "constituency_id": "int32",
            "constituency_name": "string",
        }
    )
    return data


def get_measures_data() -> pd.DataFrame:
    print("reading csv")
    file = Path("air_quality_monitoring\\data\\measures.csv")
    data = pd.read_csv(
        file,
        encoding="utf8",  # specify encoding because of the special chars in the units
    )
    print("setting datatypes")
    data = data.astype(
        {
            "measure": "string",
            "desc": "string",
            "unit": "string",
        }
    )
    return data


def get_constituency_geospacial(df_consts: pd.DataFrame):
    # create an empty list to hold geopandas geodataframes
    consituency_gdfs = []

    # create on gdf for each geojson
    # these geojsons were found on mapit.com
    for file in [
        Path("air_quality_monitoring\\data\\constituency_geoms\\bristol_east.geojson"),
        Path("air_quality_monitoring\\data\\constituency_geoms\\bristol_north.geojson"),
        Path("air_quality_monitoring\\data\\constituency_geoms\\bristol_south.geojson"),
        Path("air_quality_monitoring\\data\\constituency_geoms\\bristol_west.geojson"),
    ]:
        df = gpd.read_file(file)
        df["constituency_name"] = str.replace(file.stem, "_", " ")
        consituency_gdfs.append(df)

    # combine to one pandas dataframe, then turn this back into a geodataframe
    # specify crs - this one is for longitude and latitude coordinates
    gdf_constituencies = gpd.GeoDataFrame(
        pd.concat(consituency_gdfs, ignore_index=True), crs="EPSG:4326"
    )

    # combine the data read from the table with the geospatial data
    final_constituencies = gpd.GeoDataFrame(
        pd.merge(df_consts, gdf_constituencies, on="constituency_name")
    )
    return final_constituencies


def get_station_geospacial(stations_df):
    points = []
    for _, row in stations_df.iterrows():
        point = shp.Point(row["longitude"], row["latitude"])
        points.append(point)

    geometries = pd.Series(points)
    geostations = gpd.GeoDataFrame(stations_df, geometry=geometries, crs="EPSG:4326")
    return geostations


def get_stations_data() -> pd.DataFrame:
    constituencies = get_constituencies_data()
    geo_constituencies = get_constituency_geospacial(constituencies)

    stations_file = Path("air_quality_monitoring\\data\\stations.csv")
    stations = pd.read_csv(
        stations_file, names=["site_id", "station_name", "latitude", "longitude"]
    )
    geo_stations = get_station_geospacial(stations)

    # use a spatial join to join stations to consituencies where the coordinates of the station are within the polygon of the constituency
    final_stations = geo_stations.sjoin(
        geo_constituencies[["constituency_id", "geometry"]],
        how="inner",
        predicate="within",
    )
    final_stations = final_stations.drop(["index_right", "geometry"], axis=1)
    return final_stations[["site_id", "station_name", "constituency_id", "latitude", "longitude"]]


# sad i couldn't get this to work :(

# def ingest_data(data: pd.DataFrame):
#     print("setting up sender")
#     conf = "http::addr=localhost:9000;"
#     with Sender.from_conf(conf) as sender:
#         print("sending...")
#         sender.dataframe(data, table_name="readings", at="Date_Time")

#     print("sent")


def import_readings_parquet(data: pd.DataFrame):
    # create the path object. Delete the file if it already exists, and then add the parquet to the import folder
    # we delete first so that if changes are made to the data, such as the names of the columns of the number of rows,
    # then this whole script can just be run over again. this way, the behavouir of the script is the same every time it runs.
    filepath = Path(r"air_quality_monitoring\questdb_volume\import\air_quality.parquet")
    if filepath.exists():
        os.remove(filepath)
    data.to_parquet(path=filepath)

    # similar to why we delete an existing parquet file, we want to drop the table if its already been created before.
    drop_table = """drop table if exists readings;"""
    params = {"query": drop_table}
    try:
        drop_response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the drop statement to quest: {e}")
        raise e
    except ConnectionRefusedError as ce:
        print(  # i did this a lot, so print an explicit warning for not starting and running the quest image
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(drop_response.status_code)
    # raise an exeption and stop the script if we get a bad response from quest.
    if drop_response.status_code != 200:
        # this is normally because the query syntax is invalid, we print the response content in case it contains information
        # that will help us debug this
        print(drop_response.content)
        raise Exception(
            f"recieved status code {drop_response.status_code} from quest db for drop statement"
        )

    # as above, now for the copy into
    # we have to name each column instead of using '*' because we are ensure the type of the timestamp
    copy_into = """
    create table
        readings
    as (
        select
            cast(Date_Time as timestamp) as datetime 
            ,Site_ID
            ,NOx
            ,NO2
            ,NO
            ,PM10
            ,O3
            ,Temperature
            ,reading_id
            ,NVPM10
            ,VPM10
            ,NVPM2_5
            ,PM2_5
            ,VPM2_5
            ,CO
            ,RH
            ,Pressure
            ,SO2
        from
            read_parquet('air_quality.parquet')
        order by 
            datetime asc
        )
    timestamp(datetime);"""
    params = {"query": copy_into}
    try:
        response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the copy into statement to quest: {e}")
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(response.status_code)
    if response.status_code != 200:
        print(response.content)
        raise Exception(f"recieved status code {response.status_code} from quest db")


def import_constituency_data(data: pd.DataFrame):
    filepath = Path(r"air_quality_monitoring\questdb_volume\import\constituencies.parquet")
    if filepath.exists():
        os.remove(filepath)
    data.to_parquet(path=filepath)

    drop_table = """drop table if exists constituencies;"""
    params = {"query": drop_table}
    try:
        drop_response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the drop statement to quest: {e}")
        raise e
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(drop_response.status_code)
    if drop_response.status_code != 200:
        print(drop_response.content)
        raise Exception(
            f"recieved status code {drop_response.status_code} from quest db for drop statement"
        )

    copy_into = """
    create table
        constituencies
    as (
        select
            *
        from
            read_parquet('constituencies.parquet')
        );"""
    params = {"query": copy_into}
    try:
        response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the copy into statement to quest: {e}")
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(response.status_code)
    if response.status_code != 200:
        print(response.content)
        raise Exception(f"recieved status code {response.status_code} from quest db")


def import_measures_data(data: pd.DataFrame):
    filepath = Path(r"air_quality_monitoring\questdb_volume\import\measures.parquet")
    if filepath.exists():
        os.remove(filepath)
    data.to_parquet(path=filepath)

    drop_table = """drop table if exists measures;"""
    params = {"query": drop_table}
    try:
        drop_response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the drop statement to quest: {e}")
        raise e
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(drop_response.status_code)
    if drop_response.status_code != 200:
        print(drop_response.content)
        raise Exception(
            f"recieved status code {drop_response.status_code} from quest db for drop statement"
        )

    copy_into = """
    create table
        measures
    as (
        select
            *
        from
            read_parquet('measures.parquet')
        );"""
    params = {"query": copy_into}
    try:
        response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the copy into statement to quest: {e}")
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(response.status_code)
    if response.status_code != 200:
        print(response.content)
        raise Exception(f"recieved status code {response.status_code} from quest db")


def import_stations_data(data: pd.DataFrame):
    filepath = Path(r"air_quality_monitoring\questdb_volume\import\stations.parquet")
    if filepath.exists():
        os.remove(filepath)
    data.to_parquet(path=filepath, index=False)

    drop_table = """drop table if exists stations;"""
    params = {"query": drop_table}
    try:
        drop_response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the drop statement to quest: {e}")
        raise e
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(drop_response.status_code)
    if drop_response.status_code != 200:
        print(drop_response.content)
        raise Exception(
            f"recieved status code {drop_response.status_code} from quest db for drop statement"
        )

    copy_into = """
    create table
        stations
    as (
        select
            *
        from
            read_parquet('stations.parquet')
        );"""
    params = {"query": copy_into}
    try:
        response = requests.get(url="http://127.0.0.1:9000/exec", params=params)
    except requests.HTTPError as e:
        print(f"a http error occured when sending the copy into statement to quest: {e}")
    except ConnectionRefusedError as ce:
        print(
            "the connection was refused - please check that the quest db instance is running, and consider starting the docker image using the instructions in README.md"
        )
        raise ce
    print(response.status_code)
    if response.status_code != 200:
        print(response.content)
        raise Exception(f"recieved status code {response.status_code} from quest db")


if __name__ == "__main__":
    data = get_readings_data()
    # ingest_data(data)
    import_readings_parquet(data)

    data = get_stations_data()
    import_stations_data(data)

    data = get_measures_data()
    import_measures_data(data)

    data = get_constituencies_data()
    import_constituency_data(data)

    print("done")
