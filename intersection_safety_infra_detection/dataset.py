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
        except ValueError:
            continue
        except:
            print(
                "Unknown error with station:",
                (station_row["Latitude"], station_row["Longitude"]),
            )

        # iterate through nodes in new graph
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
def existing_stations(
    input_path: Path = RAW_DATA_DIR / "intersections.csv",
    output_path: Path = INTERIM_DATA_DIR / "existing_stations_intersections.csv",
):
    all_intersections = pd.read_csv(input_path, index_col=0)
    all_intersections = all_intersections[
        all_intersections["buffer"] == "Existing Transit"
    ]

    all_intersections.to_csv(output_path)


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR / "dataset.csv",
    output_path: Path = INTERIM_DATA_DIR / "dataset.csv",
    # ----------------------------------------------
):
    # # ---- REPLACE THIS WITH YOUR OWN CODE ----
    # logger.info("Processing dataset...")
    # for i in tqdm(range(10), total=10):
    #     if i == 5:
    #         logger.info("Something happened for iteration 5.")
    # logger.success("Processing dataset complete.")
    # # -----------------------------------------
    pass


if __name__ == "__main__":
    app()
