from pathlib import Path

# from requests import get
# import zipfile
# import csv
# from pprint import pprint
# from datetime import datetime
import pandas as pd


def get_datafile():
    r"C:\Users\Daniel\Documents\dmf\assignment\Air_Quality_Continuous.csv"
    datafile = Path("Air_Quality_Continuous.csv")

    # ToDo Fix Me
    # if not datafile.exists():
    #     url = r"https://fetstudy.uwe.ac.uk/~p-chatterjee/2023-24/dmf/Air_Quality_Continuous.zip"
    #     file_response = get(url)
    #     zip_file = Path("air_quality.zip")

    #     if file_response.status_code == 200:
    #         with open(datafile, "w") as file:
    #             file.write(file_response.content)

    #     if file_response.status_code == 200:
    #         with open(zip_file, 'w') as file:
    #             file.write(file_response.content)
    #     else:
    #         raise Exception(f"could not download the file - recived response status {file_response.status_code}")

    #     # with zipfile.ZipFile(file_response.content, 'r') as zip_ref:
    #     #     zip_ref.extractall(datafile)

    return datafile


def read_datafile() -> pd.DataFrame:
    datafile = get_datafile()
    air_quality_data = pd.read_csv(
        datafile,
        delimiter=",",
        header=0,
    )

    return air_quality_data


def clean_data(air_quality_data: pd.DataFrame) -> pd.DataFrame:
    # change data type for timestamp columns so we can us comparisons to crop the timeframe
    air_quality_data["Date_Time"] = air_quality_data["Date_Time"].astype("datetime64[s, UTC]")

    # if the site id is null, we want to remove the reading from the dataset
    df_air_quality = air_quality_data.dropna(subset=["Site_ID"])
    df_air_quality = df_air_quality.astype({"Site_ID": "int64"})

    # these show us there are no valid data points in the object1 column, so we will drop it
    print(df_air_quality.describe())
    print(df_air_quality.info())
    df_air_quality = df_air_quality.drop(columns="ObjectId")

    recent_data = df_air_quality[
        df_air_quality["Date_Time"] >= pd.Timestamp("2015-01-01", tz="UTC")
    ]
    cropped_data = recent_data[recent_data["Date_Time"] <= pd.Timestamp("2023-10-22", tz="UTC")]

    # the objectId2 value is always unique, so we can rename it reading_id to be used as our PK
    cropped_data = cropped_data.rename(
        mapper={
            "ObjectId2": "reading_id",
        },
        axis=1,
    )
    return cropped_data


def write_data_to_csv(data: pd.DataFrame) -> Path:
    cropped_datafile = Path("air_quality_cropped.csv")
    data.to_csv(cropped_datafile, index=False, header=True)
    return cropped_datafile


if __name__ == "__main__":
    raw_df = read_datafile()
    cropped_df = clean_data(raw_df)
    write_data_to_csv(data=cropped_df)
