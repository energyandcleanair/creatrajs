# creatrajs
`creatrajs` is a R package dedicated to calculating trajectories (or dispersion) and performing operation on them. It builds upon `splitr` package.

`creatrajs` also holds a few important **fire-related functions** (downloading fire data from FIRMS, attaching fires to trajectories etc.).

### Setup

Environment variables required:
```{bash}
# Keys
EOSDIS_TOKEN= # To download FIRMS fire data
GOOGLE_MAP_API_KEY=

# Directories
DIR_FIRMS= # Folder where FIRMS data will be downloaded
DIR_HYSPLIT_MET= # Folder where Hysplit meteorological data will be downloaded

# Google Cloud
GCS_DEFAULT_BUCKET=
GCS_AUTH_FILE=

# For later
EARTHDATA_USR=
EARTHDATA_PWD=
GDAL_PATH=
```

## Download archive fire data
The script can automatically download recent data from https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/.

However this data is only available for the last 60 days. To download older data (e.g. in case the script didn't run for a while, or the token expired),
go to https://firms.modaps.eosdis.nasa.gov/download/create.php

Select the following options:
- Region: Global
- Source: VIIRS S-NPP
- Format: csv

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


#### Protobuf

If you're facing `google/protobuf/port_def.inc: No such file or directory`:
```bash
conda install protobuf=3.10.1
```


