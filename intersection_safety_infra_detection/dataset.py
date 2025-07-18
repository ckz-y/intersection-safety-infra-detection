from pathlib import Path

import numpy as np
import osmnx as ox
import pandas as pd
import requests
import typer
from config import INTERIM_DATA_DIR, RAW_DATA_DIR

# from intersection_safety_infra_detection.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from geopy import distance
from loguru import logger
from osmnx.utils_geo import bbox_from_point
from pyproj import Transformer
from tqdm import tqdm
from PIL import Image
from io import BytesIO  # To read image from bytes

app = typer.Typer()
logger.add("logs/dataset_{time}.log", backtrace=True, diagnose=False)


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
    output_data.to_csv(output_path, index=False)

    logger.success("Intersections generated.")


@app.command()
def filter_and_format_data(
    input_path: Path = INTERIM_DATA_DIR / "intersections.csv",
    output_path: Path = INTERIM_DATA_DIR / "existing_stations_intersections.csv",
):
    all_intersections = pd.read_csv(input_path)
    all_intersections = all_intersections[
        all_intersections["Buffer"] == "Existing Transit"
    ]
    all_intersections = all_intersections[
        ~all_intersections["Agency"].str.contains("Amtrak")
    ]

    all_intersections = all_intersections.reset_index(names="Station_ID")
    all_intersections["temp_col"] = (
        all_intersections["Inter_Latitude"].astype(str)
        + "_"
        + all_intersections["Inter_Longitude"].astype(str)
    )
    all_intersections["Intersection_ID"] = all_intersections["temp_col"].factorize()[0]
    all_intersections = all_intersections.drop(["temp_col", "Buffer"], axis=1)

    for i in range(2015, 2025):
        all_intersections[str(i)] = 0

    all_intersections.to_csv(output_path, index=False)


@app.command()
def download_images(
    databases_path: Path = RAW_DATA_DIR / "data_sources.csv",
    image_path: Path = RAW_DATA_DIR / "images",
    data_availability_init_path: Path = INTERIM_DATA_DIR / "data_availability.csv",
    data_availability_path: Path = INTERIM_DATA_DIR / "data_availability.csv",
):
    intersections = pd.read_csv(data_availability_init_path)
    data_sources = pd.read_csv(databases_path)

    for data_source in tqdm(
        data_sources.itertuples(index=False), total=len(data_sources)
    ):
        source_url = str(data_source.URL)
        source_year = str(data_source.Year)

        logger.info("Starting source " + source_url)

        if source_url.startswith(
            "https://vginmaps.vdem.virginia.gov/arcgis/rest/services/VBMP_Imagery"
        ):
            server_bbox = [36.5, -84, 39.5, -75]  # Virginia bounding box
        elif source_url.startswith(
            "https://imagery.pasda.psu.edu/arcgis/rest/services/pasda/PhiladelphiaImagery"
        ):
            server_bbox = [39.7, -75.5, 40.2, -74.8]
        else:
            try:
                source_request = requests.get(source_url, {"f": "json"}, timeout=10)
                source_extent = source_request.json()["fullExtent"]
            except:
                logger.exception(
                    "Error during extent query. Moving on to next data source."
                )
            else:
                try:
                    source_wkid = source_extent["spatialReference"]["latestWkid"]
                except:
                    source_wkid = source_extent["spatialReference"]["wkid"]
                if source_wkid in (3857, 102100):
                    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326")
                    server_bbox = list(
                        transformer.transform(
                            source_extent["xmin"], source_extent["ymin"]
                        )
                    ) + list(
                        transformer.transform(
                            source_extent["xmax"], source_extent["ymax"]
                        )
                    )
                elif source_wkid == 4326:
                    server_bbox = [
                        source_extent["xmin"],
                        source_extent["ymin"],
                        source_extent["xmax"],
                        source_extent["ymax"],
                    ]
                else:
                    logger.exception(
                        "Data source with unknown spatial reference. Moving on to next data source."
                    )
                    continue

            # Identify intersections which belong to server bounding box.
            intersections_to_query = ~(
                (intersections["Inter_Latitude"] < server_bbox[0])  # type: ignore
                | (intersections["Inter_Latitude"] > server_bbox[2])  # type: ignore
                | (intersections["Inter_Longitude"] < server_bbox[1])  # type: ignore
                | (intersections["Inter_Longitude"] > server_bbox[3])  # type: ignore
            ) & (intersections[source_year] == 0)

            bounding_boxes = list(
                intersections[intersections_to_query].apply(
                    lambda x: ",".join(
                        str(i)
                        for i in bbox_from_point(
                            point=(
                                x["Inter_Latitude"],
                                x["Inter_Longitude"],
                            ),
                            dist=30,
                        )
                    ),
                    axis=1,
                )
            )

            for i, intersection in tqdm(
                enumerate(
                    intersections[intersections_to_query].itertuples(index=False)
                ),
                total=len(intersections[intersections_to_query]),
            ):
                intersection_bbox = bounding_boxes[i]
                params = {
                    "bbox": intersection_bbox,
                    "size": "416,416",
                    "f": "image",
                    "bboxSR": str(4326),
                }

                try:
                    r = requests.get(source_url + "/export", params, timeout=10)
                    r.raise_for_status()
                except requests.exceptions.HTTPError:
                    try:
                        r = requests.get(
                            source_url + "/exportImage", params, timeout=10
                        )
                        r.raise_for_status()
                    except requests.exceptions.HTTPError:
                        logger.exception(
                            "HTTPError with intersection "
                            + str(intersection.Intersection_ID)
                        )
                    except requests.exceptions.Timeout:
                        logger.exception("API timeout. Moving on to next data source.")
                        break
                    except:
                        logger.exception(
                            "Unknown error with url '/export' and intersection "
                            + str(intersection.Intersection_ID)
                        )
                except requests.exceptions.Timeout:
                    logger.exception("API timeout. Moving on to next data source.")
                    break
                except:
                    logger.exception(
                        "Unknown error with url '/export' and intersection "
                        + str(intersection.Intersection_ID)
                    )

                image_file_extrema = Image.open(BytesIO(r.content)).getextrema()  # type: ignore

                # skip if image is white: set value to 1
                if ((image_file_extrema[0] == 0) and (image_file_extrema[1] == 0)) or (
                    image_file_extrema == ((0, 0), (0, 0), (0, 0), (0, 0))
                ):
                    intersections.loc[
                        intersections["Intersection_ID"]
                        == intersection.Intersection_ID,
                        source_year,
                    ] = 1
                    try:
                        intersections.to_csv(data_availability_path, index=False)
                    except KeyboardInterrupt:
                        intersections.to_csv(data_availability_path, index=False)
                        logger.info(
                            "Saved successfully. Exiting due to keyboard interupt."
                        )
                        raise SystemExit(0)
                    except:
                        logger.error("Error saving data availability file.")
                    continue

                try:
                    with open(
                        image_path
                        / (
                            str(intersection.Intersection_ID)
                            + "_"
                            + source_year
                            + ".png"
                        ),
                        "wb",
                    ) as fd:
                        fd.write(r.content)  # type: ignore

                    intersections.loc[
                        intersections["Intersection_ID"]
                        == intersection.Intersection_ID,
                        source_year,
                    ] = 2

                    intersections.to_csv(data_availability_path, index=False)
                except KeyboardInterrupt:
                    with open(
                        image_path
                        / (
                            str(intersection.Intersection_ID)
                            + "_"
                            + source_year
                            + ".png"
                        ),
                        "wb",
                    ) as fd:
                        fd.write(r.content)  # type: ignore

                    intersections.loc[
                        intersections["Intersection_ID"]
                        == intersection.Intersection_ID,
                        source_year,
                    ] = 2

                    intersections.to_csv(data_availability_path, index=False)
                    logger.info("Saved successfully. Exiting due to keyboard interupt.")
                    raise SystemExit(0)
                except:
                    logger.error(
                        "Saving interupted. Intersection ID"
                        + str(intersection.Intersection_ID)
                    )
        logger.info("Successfully complete source " + source_url)

    logger.success("Images downloading complete.")


@app.command()
def main():
    # generate_intersections()
    # filter_and_format_data()
    print("test")


if __name__ == "__main__":
    app()
    # download_images() # for testing
