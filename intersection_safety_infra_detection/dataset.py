from pathlib import Path

from loguru import logger
from tqdm import tqdm
import typer
import pandas as pd
import numpy as np
import osmnx as ox
from geopy import distance

# from intersection_safety_infra_detection.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from config import RAW_DATA_DIR, INTERIM_DATA_DIR

app = typer.Typer()


@app.command()
def generate_intersections(
    input_path: Path = RAW_DATA_DIR / "tod_database_download.csv",
    output_path: Path = INTERIM_DATA_DIR / "intersections.csv",
):
    station_data = pd.read_csv(input_path)

    intersection_data_cols = station_data.columns.to_list()
    intersection_data_cols[5:7] = ["Station_Latitude", "Station_Longitude"]
    intersection_data_cols.extend(["Inter_Latitude", "Inter_Longitude"])

    intersection_data_list = []

    logger.info("Generating intersections...")
    for _, station_row in tqdm(station_data.iterrows(), total=len(station_data)):
        try:
            station_local_network = ox.graph.graph_from_point(
                (station_row["Latitude"], station_row["Longitude"]),
                dist=250,
                dist_type="bbox",
                network_type="drive",
                retain_all=True,
            )
        # No intersections found in bounding box.
        except ValueError:
            continue
        except:
            logger.exception(
                "Unknown error with station:",
                (station_row["Latitude"], station_row["Longitude"]),
            )

        station_lat = station_row["Latitude"]
        station_lon = station_row["Longitude"]
        station_row = station_row.to_list()

        for _, intersection_data in station_local_network.nodes.items():  # type: ignore
            new_intersection = station_row.copy()
            intersection_lat = intersection_data["y"]
            intersection_lon = intersection_data["x"]

            if (
                distance.great_circle(
                    (station_lat, station_lon), (intersection_lat, intersection_lon)
                ).meters
                <= 250
            ):
                new_intersection.extend([intersection_lat, intersection_lon])
                intersection_data_list.append(new_intersection)

    output_data = pd.DataFrame(
        data=intersection_data_list, columns=intersection_data_cols
    )
    output_data.to_csv(output_path)

    logger.success("Intersections generated.")


@app.command()
def filter_and_format_data(
    input_path: Path = INTERIM_DATA_DIR / "intersections.csv",
    output_path: Path = INTERIM_DATA_DIR / "existing_stations_intersections.csv",
):
    all_intersections = pd.read_csv(input_path, index_col=0)
    all_intersections = all_intersections[
        all_intersections["Buffer"] == "Existing Transit"
    ]

    for i in range(2015, 2025):
        all_intersections[str(i)] = np.nan

    all_intersections = all_intersections.reset_index(names="Station_ID")
    all_intersections["temp_col"] = (
        all_intersections["Inter_Latitude"].astype(str)
        + "_"
        + all_intersections["Inter_Longitude"].astype(str)
    )
    all_intersections["Intersection_ID"] = all_intersections["temp_col"].factorize()[0]
    all_intersections.drop("temp_col", axis=1)
    all_intersections.to_csv(output_path)


@app.command()
def main():
    generate_intersections()
    filter_and_format_data()


if __name__ == "__main__":
    app()
