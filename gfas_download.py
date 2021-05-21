#!/usr/bin/env python
import argparse
from ecmwfapi import ECMWFDataServer
from dotenv import load_dotenv
from datetime import date
from datetime import datetime
from datetime import timedelta

import os

load_dotenv(".Renviron")

parser = argparse.ArgumentParser(description='Download GFAS PM2.5 emission nc files')
parser.add_argument('date_from', type=str,
                    help='Date from e.g. 2020-01-31')
parser.add_argument('date_to', type=str,
                    help='Date to (included) e.g. 2020-02-13')

parser.add_argument('--url', type=str, nargs='?',
                    default=os.environ["ECMWF_API_URL"],
                    help='Date to (included) e.g. 2020-02-13')

parser.add_argument('--key', type=str, nargs='?',
                    default=os.environ["ECMWF_API_KEY"],
                    help='Date to (included) e.g. 2020-02-13')

parser.add_argument('--email', type=str, nargs='?',
                    default=os.environ["ECMWF_API_EMAIL"],
                    help='Date to (included) e.g. 2020-02-13')

args = parser.parse_args()

print(args.date_from)

server = ECMWFDataServer(url=args.url,
                         key=args.key,
                         email=args.email)

# date_from=datetime.strptime("2020-01-01","%Y-%m-%d")
# date_to=datetime.strptime("2020-12-31","%Y-%m-%d")

filename = args.date_from.replace("-", "") + "_" + args.date_to.replace("-", "") + ".nc"

server.retrieve({
    "class": "mc",
    "dataset": "cams_gfas",
    "date": args.date_from + "/to/" + args.date_to,
    "expver": "0001",
    "levtype": "sfc",
    "param": "87.210",
    "step": "0-24",
    "stream": "gfas",
    "time": "00:00:00",
    "type": "ga",
    "target": filename,
    "format": "netcdf"
})