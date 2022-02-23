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



### Known Issues

If trajectories returned are empty:

- check paths are absolute and without ~
- `sudo apt-get install libgfortran3`


