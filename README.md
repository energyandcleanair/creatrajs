# creatrajs

R package for computing back-trajectories and dispersions, downloading fire data, attaching fire to trajectories, and caching results in MongoDB.

The trajectory computation leverages [`splitr`](https://github.com/rich-iannone/splitr) (HYSPLIT wrapper).

## What it does

**Trajectories** (`trajs.R`, `dispersion.R`)
- Compute HYSPLIT back-trajectories and dispersion models for a set of locations and dates
- Upload/download trajectories to/from MongoDB GridFS (`db.R`)

**Fire attachment** (`fire.R`, `gfas.R`)
- Download VIIRS/FIRMS active fire data from EOSDIS (near-real-time, last 60 days) or archive
- Attach fire observations to trajectories, circular extents, or dispersion rasters
- Aggregate fire counts/FRP within geographic extents

**MODIS FRP** (`utils.modis.R`) — _experimental, not part of normal workflow_
- Download and process MODIS Terra/Aqua fire radiative power rasters
- Requires separate MODIS data directory and EarthData credentials

## Setup

Environment variables:
```bash
# Keys
EOSDIS_TOKEN=        # NASA FIRMS API token (https://firms.modaps.eosdis.nasa.gov/api/area/)
GOOGLE_MAP_API_KEY=  # For fire heatmap visualisation (optional)

# Directories
DIR_FIRMS=           # Local folder for FIRMS fire data
DIR_HYSPLIT_MET=     # Local folder for HYSPLIT met data

# Google Cloud (for trajectory export via run_and_export.R)
GCS_DEFAULT_BUCKET=
GCS_AUTH_FILE=

# MODIS (experimental, not used in normal workflow)
EARTHDATA_USR=
EARTHDATA_PWD=
GDAL_PATH=
DIR_MODIS=
```

## Fire archive data

Near-real-time FIRMS data covers only the **last 60 days**. For older data (e.g. after a token expiry or a gap in runs):

1. Go to https://firms.modaps.eosdis.nasa.gov/download/create.php
2. Select: Region = Global, Source = VIIRS S-NPP, Format = CSV
3. Place downloaded files in `$DIR_FIRMS/suomi-npp-viirs-c2/Global/`
4. Run `fire.split_archive()` to split multi-day files into daily files

Once the request is processed, you can use the following script to download, extract and split the data:

```
# Make sure the firms folder is correct
stopifnot(creatrajs::utils.get_firms_folder() != "")


url <- "https://firms.modaps.eosdis.nasa.gov/data/download/DL_FIRE_SV-C2_685973.zip" # REPLACE WITH YOU URL
tmp <- tempfile(fileext = ".zip")
download.file(url, tmp, mode = "wb")
unzip(tmp, exdir = tempdir())
list.files(tempdir())

files <- list.files(tempdir(), pattern = "fire_.*.csv$", full.names = TRUE)
lapply(files, creatrajs::fire.split_archive)

# Empty tmpdir in case you have more than one request
unlink(list.files(tempdir(), full.names = TRUE))

```

### Known Issues

If trajectories returned are empty:

- check paths are absolute and without ~
- `sudo apt-get install libgfortran3`

#### Libgfortran3 not available
To install `libgfortran3` on Ubuntu 20.04+:
```bash
wget http://archive.ubuntu.com/ubuntu/pool/main/g/gcc-5/libgfortran3_5.4.0-6ubuntu1~16.04.12_amd64.deb
sudo dpkg -i libgfortran3_5.4.0-6ubuntu1~16.04.12_amd64.deb

```