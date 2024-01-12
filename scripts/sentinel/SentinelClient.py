import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import requests
from requests.exceptions import ChunkedEncodingError


class SentinelClient:
    def __init__(self, config):
        self.mission = config.mission
        self.user = config.user
        self.passwd = config.secret
        self.workdir = config.workdir
        self.out_dir = config.out_dir
        self.session = requests.Session()

    def get_keycloak(self, config):
        """Function for generating a key token for the Sentinel dataspace"""
        data = {
            "client_id": "cdse-public",
            "username": config.user,
            "password": config.secret,
            "grant_type": "password",
        }
        try:
            r = self.session.post(
                "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                data=data,
            )
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(
                f"Keycloak token creation failed, check your login credentials. Error: {e}"
            )
            raise Exception(
                f"Keycloak token creation failed, check your login credentials. Error: {e}"
            )

        try:
            return r.json()["access_token"]
        except (ValueError, KeyError) as e:
            logging.error(f"Error parsing Keycloak response: {e}")
            raise Exception(f"Error parsing Keycloak response: {e}")

    def __str__(self):
        return (
            f"SentinelClient Information:\n"
            f"Mission: {self.mission}\n"
            f"User: {self.user}\n"
            f"Working Directory: {self.workdir}\n"
            f"Output Directory: {self.out_dir}\n"
        )

    @dataclass
    class SentinelProduct:
        id: str
        name: str
        content_type: str
        content_length: int
        origin_date: str
        publication_date: str
        modification_date: str
        online: bool
        eviction_date: str
        s3_path: str
        checksum: List[str]
        content_date_start: str
        content_date_end: str
        footprint: str
        geo_footprint: dict

        def __str__(self):
            return (
                f"SentinelProduct Information:\n"
                f"ID: {self.id}\n"
                f"Name: {self.name}\n"
                f"Content Type: {self.content_type}\n"
                f"Content Length: {self.content_length}\n"
                f"Origin Date: {self.origin_date}\n"
                f"Publication Date: {self.publication_date}\n"
                f"Modification Date: {self.modification_date}\n"
                f"Online: {self.online}\n"
                f"Eviction Date: {self.eviction_date}\n"
                f"S3 Path: {self.s3_path}\n"
                f"Checksum: {', '.join(self.checksum)}\n"
                f"Content Date Start: {self.content_date_start}\n"
                f"Content Date End: {self.content_date_end}\n"
                f"Footprint: {self.footprint}\n"
                f"Geo Footprint: {self.geo_footprint}\n"
            )

    def create_sentinel_product(self, item):
        return self.SentinelProduct(
            id=item["Id"],
            name=item["Name"],
            content_type=item["ContentType"],
            content_length=item["ContentLength"],
            origin_date=item["OriginDate"],
            publication_date=item["PublicationDate"],
            modification_date=item["ModificationDate"],
            online=item["Online"],
            eviction_date=item["EvictionDate"],
            s3_path=item["S3Path"],
            checksum=item["Checksum"],
            content_date_start=item["ContentDate"]["Start"],
            content_date_end=item["ContentDate"]["End"],
            footprint=item["Footprint"],
            geo_footprint=item["GeoFootprint"],
        )

    @retry
    def search_products(self, config):
        """Search the Copernicus dataspace for images according to the filters"""
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                if config.download_type == "ingestion":
                    acquisition_start = (
                        datetime.strptime(config.start_time, "%Y-%m-%dT%H:%M:%SZ")
                        - timedelta(days=30)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    if config.mission == "SENTINEL-1":
                        response = self.session.get(
                            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{config.mission}' "
                            f"and contains(Name,'_{config.product_mode}_') "
                            f"and contains(Name,'1SDV') "
                            f"and OData.CSC.Intersects(area=geography'SRID=4326;{config.geometry.wkt}') and PublicationDate gt {config.start_time} "
                            f"and PublicationDate lt {config.end_time} "
                            f"and ContentDate/Start gt {acquisition_start} "
                            f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
                            f"and att/OData.CSC.StringAttribute/Value eq '{config.product_type}')&$top=100"
                        )
                    elif config.mission == "SENTINEL-2":
                        response = self.session.get(
                            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{config.mission}' "
                            f"and OData.CSC.Intersects(area=geography'SRID=4326;{config.geometry.wkt}') and PublicationDate gt {config.start_time} "
                            f"and PublicationDate lt {config.end_time} "
                            f"and ContentDate/Start gt {acquisition_start} "
                            f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
                            f"and att/OData.CSC.StringAttribute/Value eq '{config.product_type}')&$top=100"
                        )
                    else:
                        logging.error(
                            "Choose one of SENTINEL-1 or SENTINEL-2 for the mission"
                        )
                        raise ValueError(
                            "Choose one of SENTINEL-1 or SENTINEL-2 for the mission"
                        )
                    response.raise_for_status()
                elif config.download_type == "acquisition":
                    if config.mission == "SENTINEL-1":
                        response = self.session.get(
                            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{config.mission}' "
                            f"and contains(Name,'_{config.product_mode}_') "
                            f"and contains(Name,'1SDV') "
                            f"and OData.CSC.Intersects(area=geography'SRID=4326;{config.geometry.wkt}') "
                            f"and ContentDate/Start gt {config.start_time} "
                            f"and ContentDate/Start lt {config.end_time} "
                            f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
                            f"and att/OData.CSC.StringAttribute/Value eq '{config.product_type}')&$top=100"
                        )
                    elif config.mission == "SENTINEL-2":
                        response = self.session.get(
                            f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{config.mission}' "
                            f"and OData.CSC.Intersects(area=geography'SRID=4326;{config.geometry.wkt}') "
                            f"and ContentDate/Start gt {config.start_time} "
                            f"and ContentDate/Start lt {config.end_time} and "
                            f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
                            f"and att/OData.CSC.StringAttribute/Value eq '{config.product_type}')&$top=100"
                        )
                    else:
                        logging.error(
                            "Choose one of SENTINEL-1 or SENTINEL-2 for the mission"
                        )
                        raise ValueError(
                            "Choose one of SENTINEL-1 or SENTINEL-2 for the mission"
                        )
                    response.raise_for_status()
                else:
                    logging.error(
                        "Choose one of acquistion or ingestion for the download_type"
                    )
                    raise ValueError(
                        "Choose one of acquisition or ingestion for the download_type"
                    )
                break
            except ChunkedEncodingError as e:
                if attempt == max_retries:  # If max retries reached, raise error
                    logging.error(
                        f"Connection error: Max amount of retries ({max_retries}) reached. Error: {e}"
                    )
                    raise Exception(
                        f"Connection error: Max amount of retries ({max_retries}) reached. Error: {e}"
                    )
                else:
                    logging.info(
                        f"Connection error, Error: {e}, attempt number {attempt}, retrying"
                    )
                    continue
            except requests.exceptions.RequestException as e:
                logging.error(f"Product search failed. Error: {e}")
                raise Exception(f"Product search failed. Error: {e}")

        try:
            sentinel_products = [
                self.create_sentinel_product(item)
                for item in response.json().get("value", [])
            ]
        except (ValueError, KeyError) as e:
            logging.error(f"Error parsing product search response: {e}")
            raise Exception(f"Error parsing product search response: {e}")

        logging.info(
            f"Returned product IDs: {[product.id for product in sentinel_products]}"
        )
        logging.info(
            f"Returned product names: {[product.name for product in sentinel_products]}"
        )
        if not sentinel_products:
            logging.info("No products found for given search criteria")
        return sentinel_products

    def download_products(self, config):
        """Download the images found"""
        max_retries = 3
        # try:
        sentinel_products = self.search_products(config)
        product_ids = [product.id for product in sentinel_products]
        product_names = [product.name for product in sentinel_products]
        if sentinel_products:
            keycloak_token = self.get_keycloak(config)
            keycloak_gen_time = time.time()
            logging.debug("Token generated")
        for product_id, product_name in zip(product_ids, product_names):
            if (time.time() - keycloak_gen_time) > 540:
                keycloak_token = self.get_keycloak(config)
                keycloak_gen_time = time.time()
                logging.debug(
                    "Token regenerated because time limit (9 minutes) was exceeded"
                )
            self.session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
            url = (
                f"https://zipper.dataspace.copernicus.eu/odata/v1/Products("
                + product_id
                + ")/$value"
            )
            logging.info("Downloading file from: " + url)
            for attempt in range(1, max_retries + 1):
                try:
                    response = self.session.get(url, allow_redirects=False)
                    while response.status_code in (
                        301,
                        302,
                        303,
                        307,
                    ):  # URL is redirected
                        url = response.headers["Location"]
                        response = self.session.get(url, allow_redirects=False)
                    break
                except ChunkedEncodingError as e:
                    if attempt == max_retries:  # If max retries reached, raise error
                        logging.error(
                            f"Connection error: Max amount of retries ({max_retries}) reached. Error: {e}"
                        )
                        raise Exception(
                            f"Connection error: Max amount of retries ({max_retries}) reached. Error: {e}"
                        )
                    else:
                        logging.info(
                            f"Connection error, Error: {e}, attempt number {attempt}, retrying"
                        )
                        continue
                except requests.exceptions.RequestException as e:
                    logging.error(
                        f"Un unexpected error occured during download. Error: {e}"
                    )
                    raise Exception(
                        f"Un unexpected error occured during download. Error: {e}"
                    )
                time.sleep(10)
            response.raise_for_status()
            file_content = response.content

            if not os.path.exists(self.out_dir):
                os.makedirs(self.out_dir)

            with open(os.path.join(self.out_dir, product_name + ".zip"), "wb") as p:
                p.write(file_content)
            logging.info(
                "Wrote product file to: "
                + os.path.join(self.out_dir, product_name + ".zip")
            )
