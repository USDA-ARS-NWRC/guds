# GUDS - Geoserver Upload Download Script v0.1.1
[![PyPI version](https://badge.fury.io/py/guds.svg)](https://badge.fury.io/py/guds)

The GUDS repo contains a script for moving around the modeling data between its
source and the geoserver.

## Installation

### Prerequisites

* curl
* Python 3.5 +
* A geosver to push to

### Install GUDS commandline
To install GUDS, simply :

`pip install guds`

To do install it from source:

`git clone https://github.com/USDA-ARS-NWRC/guds`

`cd guds`

`pip install -r reqquirements.txt`

## Usage


`guds -f <filename> -t <upload data type>  -b <basin name> -m <mask netcdf>`

### Upload Type
GUDS is designed to handle 3 different types of data.

1. Modeled output - The modeled output should be a netcdf containing a single
day of spatial data representing the snowpack parameters. The netcdf should at
at leat contain the variables: specific_mass, thickness, snow_density

2. Topographic - To run AWSM, there is a set of static images required that describe
the envrionment to the modeling system. This file should also be a netcdf and
any images in the file will be uploaded.

3. Flights - Eventually  Lidar snow depth images will be uploadable, in the
mean time it is under development.

### Specifying the basin
To upload data, GUDS must receive a basin flag to know how to organize it.
Currently the options are:

  * brb (Boise River Basin)
  * tuolumne
  * merced
  * sanjoaquin
  * kings
  * kaweah

### Specifying Credentials
For security reasons GUDS requires a json file describing your credentials for
logging on which is assumed to be `./geoserver.json`. It should contain the
following keys:

  * url - url of the AWS instance
  * remote_username - user name on the AWS instance associated to the geoserver
  * geoserver_username - username on the geoserver
  * geoserver_password - Password for the geoserver
  * pem - Location of the .pem for accesssing the AWS instance
  * data - Location of the data folder on the server

After installing you can also run the following to get a blank credentials file.

`guds --write_json`

### Masking
A mask can be provided to mask the data. To do so use the `--mask` flag to
to pass a path to a netcdf containing a mask variable that is on the same bounds
as the uploaded data.
