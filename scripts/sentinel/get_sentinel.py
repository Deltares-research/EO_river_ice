import argparse
import glob
import json
import logging
import optparse as op
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta

from shapely.geometry import shape

from SentinelClient import SentinelClient


@dataclass
class FewsConfig:
    fews_info_file: str
    ns = {"fews": "http://www.wldelft.nl/fews/PI"}

    def __post_init__(self):
        try:
            # Parsing xml file
            xml_input = ET.parse(self.fews_info_file)
            self.root = xml_input.getroot()
        except FileNotFoundError as e:
            logging.error(f"ERROR: Reading INFO file - {str(e)}")
            exit(1)
        except Exception as e:
            logging.error(
                f"ERROR: An error occurred while parsing the INFO file: {str(e)}"
            )
            exit(1)

        # Reading directories
        prop_find = self.root.findall("fews:properties", self.ns)
        if not prop_find:
            logging.error("ERROR: No 'properties' element found in INFO file.")
            exit(1)
        props = {n.attrib["key"]: n.attrib["value"] for n in prop_find[0]}
        self.log_level = self.root.find("fews:logLevel", self.ns).text
        self.mission = props.get("mission", "")
        self.user = props.get("user", "")
        self.secret = props.get("secret", "")
        self.download_type = props.get("download_type", "")
        self.product_type = props.get("product_type", "")
        self.product_mode = props.get("product_mode", "")
        self.out_dir = props.get("destinationDir", "")
        self.workdir = self.root.find("fews:workDir", self.ns).text
        self.timezone = self.root.find("fews:timeZone", self.ns).text

        # Start logging
        loglevel = self.log_level
        numeric_level = getattr(logging, loglevel.upper(), None)
        logging.basicConfig(
            filename=os.path.join(self.workdir, "log.txt"),
            filemode="w",
            level=numeric_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            force=True,
        )
        logging.getLogger("log")
        logging.info("start logging")

        # Reading start and end time
        t0_in = self.root.find("fews:startDateTime", self.ns).attrib
        t1_in = self.root.find("fews:endDateTime", self.ns).attrib
        try:
            self.start_time = datetime.strptime(
                "{} {}".format(t0_in.get("date", ""), t0_in.get("time", "")),
                "%Y-%m-%d %H:%M:%S",
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.end_time = datetime.strptime(
                "{} {}".format(t1_in.get("date", ""), t1_in.get("time", "")),
                "%Y-%m-%d %H:%M:%S",
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            logging.error(f"ERROR: Error parsing start or end time - {str(e)}")
            raise

        # Reading geojson object
        geojson_files = glob.glob(os.path.join(self.workdir, "*.geojson"))
        if not geojson_files:
            logging.error(
                "No polygon available, please place a geojson object in the WORK directory"
            )
            raise Exception(
                "No polygon available, please place a geojson object in the WORK directory"
            )
        if len(geojson_files) > 1:
            logging.warning(
                "Multiple geojson files detected. Selecting the first file from the list."
            )
        try:
            with open(geojson_files[0]) as f:
                features = json.load(f).get("features", [])
        except json.decoder.JSONDecodeError:
            logging.error(
                "Can not decode geojson, check the file and make sure it contains a polygon"
            )
            exit(1)
        if not features:
            logging.error("Geojson does not contain a feature")
            raise Exception("Geojson does not contain a feature")
        self.geometry = shape(features[0].get("geometry"))

        if len(features) > 1:
            logging.warning(
                "Multiple geojson objects detected. Selecting the first object from the list."
            )

    def __str__(self):
        return (
            f"FewsConfig Information:\n"
            f"Log Level: {self.log_level}\n"
            f"Mission: {self.mission}\n"
            f"User: {self.user}\n"
            f"Secret: {self.secret}\n"
            f"Download Type: {self.download_type}\n"
            f"Product Type: {self.product_type}\n"
            f"Product Mode: {self.product_mode}\n"
            f"Polarization: {self.polarization}\n"
            f"Output Directory: {self.out_dir}\n"
            f"Work Directory: {self.workdir}\n"
            f"Timezone: {self.timezone}\n"
            f"Start Time: {self.start_time}\n"
            f"End Time: {self.end_time}\n"
            f"Geometry: {self.geometry}\n"
        )


def main():
    parser = argparse.ArgumentParser(description="Process FEWS configuration.")
    parser.add_argument(
        "--runInfoFile", "-r", required=True, help="Path to FEWS info file"
    )
    args = parser.parse_args()

    config = FewsConfig(args.runInfoFile)

    config = FewsConfig(args.runInfoFile)

    sentinelclient = SentinelClient(config)
    sentinelclient.search_and_download(config)


if __name__ == "__main__":
    main()
