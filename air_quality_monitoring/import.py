import pandas as pd
from pathlib import Path
from mysql import connector
from mysql.connector.abstracts import MySQLCursorAbstract
from pprint import pprint
import geopandas as gpd
import shapely as shp
import timeit

# create database connection and cursor to use for the rest of the script
DB_CONNECTION = connector.connect(
    host="127.0.0.1",
    port=3306,
    database="air_quality",
    user="root",
)


# main function for this file. it is called in the entry point at the bottom of the file. all functions used are defined below.
def main():
    start = timeit.default_timer()
    cursor = DB_CONNECTION.cursor()

    # read the constituencies data into memory and then populate the table
    constituencies_file = Path("data\\constituencies.csv")
    constituencies = populate_table_from_csv(
        cursor=cursor,
        table_name="constituencies",
        file=constituencies_file,
        column_names="(constituency_name, constituency_id)",
    )

    # read the stations data into memory and then populate the table.
    # Consituency ID is missing from the station data at the moment and will be calculated and populated in the next step.
    stations_file = Path("data\\stations.csv")
    stations = populate_table_from_csv(
        cursor=cursor,
        table_name="stations",
        file=stations_file,
        column_names="(site_id, station_name, latitude, longitude)",
    )

    # get the geospacial data of stations and constiuencies to calculate with site is in which constituency
    geo_stations = get_station_geospacial(stations)
    geo_constituencies = get_constituency_geospacial(constituencies)

    # use a spatial join to join stations to consituencies where the coordinates of the station are within the polygon of the constituency
    final_stations = geo_stations.sjoin(
        geo_constituencies[["constituency_id", "geometry"]],
        how="inner",
        predicate="within",
    )
    final_stations = final_stations.drop(["index_right", "geometry"], axis=1)

    # populate stations table with full data
    populate_table_from_df(
        cursor=cursor,
        table_name="stations",
        dataframe=final_stations,
        columns="( site_id, station_name, latitude, longitude,  constituency_id)",
    )

    # read measures data into memory then populate the table
    populate_table_from_csv(
        cursor=cursor,
        table_name="measures",
        file=Path("data\\measures.csv"),
    )

    # read the readings data into memory and populate the table.
    # this function expects the data in the correct format in the file air_quality_cropped.csv , so cropped.py must have been run first.
    populate_table_from_csv(
        cursor=cursor,
        table_name="readings",
        file=Path("data\\air_quality_cropped.csv"),
        has_headers=True,
        return_results=False,  # with a large file, returning the results takes a long time
    )

    DB_CONNECTION.close()

    end = timeit.default_timer()
    print(f"ran in {end - start}")


def insert_values(cursor: MySQLCursorAbstract, table: str, values: str, columns: str = None) -> str:
    sql = f"insert into {table} {columns} values {values};"
    cursor.execute(sql)
    DB_CONNECTION.commit()


def truncate_table(cursor: MySQLCursorAbstract, table: str) -> None:
    sql = f"truncate {table};"
    cursor.execute(sql)
    DB_CONNECTION.commit()


def select_all(cursor: MySQLCursorAbstract, table: str, limit=-1) -> list[tuple,]:
    sql = f"select * from {table}"
    cursor.execute(sql)
    if limit == -1:
        results = cursor.fetchall()
    else:
        results = cursor.fetchmany(limit)
    return results


def populate_table_from_csv(
    cursor,
    table_name: str,
    file: Path,
    has_headers=False,
    column_names="",
    return_results=True,
) -> list[tuple,] | None:
    print(f"\npopulating the table {table_name} from the file {file}\n")

    # truncate the table first so that this python script always behaves the same.
    # This is straight forward and reliable - for example, i learned that the syntax for truncating and re-inserting
    # is much simpler than updating using from values in mysql.
    truncate_table(cursor=cursor, table=table_name)

    # if too many values are sent in one statement, the connection will time out before its completed.
    # So, we can split the list of values into chunks and execute multiple inserts.
    max_rows = 5000

    # format the contents of the csv into a well-formatted string so that it can be injected into SQL
    with open(file, "r", encoding="UTF-8") as f:
        contents = f.read().rstrip()  # rstip removes any blank lines at the top or end of a file
    values = [
        "("  # open bracket for each row
        + ", ".join(
            [
                "'"  # single quote around each string
                + value.replace("'", "''")  # replace apostophes with two single quotes
                + "'"  # closing single quote around each string
                for value in line.split(",")  # do this for each comma seperated string in the row
            ]
        )  # join these quote enclose strings with commas
        + "),"  # close bracket for each row, and add a comma to seperate each row
        for line in contents.split("\n")  # do this for every row in the file
    ]
    if has_headers:
        headers = values.pop(0)  # remove the first row of values and use it as column names
    if has_headers and column_names == "":
        column_names = headers.strip(",")  # remove trailing commas
        column_names = column_names.replace(
            "'", ""
        )  # remove any apostrophies - important in the site names

    batches = [values[i : i + max_rows] for i in range(0, len(values), max_rows)]

    for batch in batches:
        batch_values = "\n".join(batch)
        batch_values = batch_values.strip(",")

        insert_values(cursor=cursor, table=table_name, values=batch_values, columns=column_names)

    if return_results:
        results = select_all(cursor=cursor, table=table_name)
        pprint(results[0:15])
        return results


def get_constituency_geospacial(constituencies: list[tuple,]):
    # create a dataframe with results - we will use it to add constituency id to stations
    df_consts = pd.DataFrame(constituencies)
    df_consts.columns = ["constituency_id", "constituency_name"]

    # create an empty list to hold geopandas geodataframes
    consituency_gdfs = []

    # create on gdf for each geojson
    # these geojsons were found on mapit.com
    for file in [
        Path("data\\constituency_geoms\\bristol_east.geojson"),
        Path("data\\constituency_geoms\\bristol_north.geojson"),
        Path("data\\constituency_geoms\\bristol_south.geojson"),
        Path("data\\constituency_geoms\\bristol_west.geojson"),
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


def get_station_geospacial(stations):
    # create a dataframe with the results, and specify the column names
    stations_df = pd.DataFrame(
        stations,
        columns=["site_id", "constituency_id", "station_name", "latitude", "longitude"],
    )
    # we will create a geometry - a Point object - for each site using the longitude and latitude coordinates. Then, we can
    # create a geodataframe which has all the information of stations plus the geometry
    points = []
    for _, row in stations_df.iterrows():
        point = shp.Point(row["latitude"], row["longitude"])
        points.append(point)

    geometries = pd.Series(points)
    geostations = gpd.GeoDataFrame(stations_df, geometry=geometries, crs="EPSG:4326")
    # remove the id from the right hand frame
    geostations = geostations.drop(["constituency_id"], axis=1)
    return geostations


def populate_table_from_df(cursor, table_name, dataframe: pd.DataFrame, columns):
    print(f"\npopulating the table {table_name} from the a dataframe\n")
    # create a formatted string of all the data that can be injected into SQL
    values = []
    for _, row in dataframe.iterrows():
        data = [str(value) for value in list(row)]
        data = "(" + ", ".join(["'" + value.replace("'", "''") + "'" for value in data]) + ")"
        values.append(data)
    values = ", ".join(values)

    # Truncating and re-inserting is more straight forward than updating when using values with MySQL,
    # updating using values was possible but the syntax that worked was very hard to read.
    truncate_table(cursor=cursor, table="stations")
    insert_values(
        cursor=cursor,
        table=table_name,
        values=values,
        columns=columns,
    )

    # select from table to check results
    results = select_all(cursor=cursor, table="stations")
    pprint(results)


if __name__ == "__main__":
    main()
