import json
import argparse
import sys
import requests
from urllib.parse import urljoin, urlparse
from shutil import copyfile, rmtree
import os
from netCDF4 import Dataset, num2date
import subprocess as sp
import logging
import coloredlogs
import certifi
from spatialnc.proj import add_proj
from spatialnc.utilities import copy_nc, mask_nc
from datetime import datetime as dt
import numpy as np
from guds import __version__
import time
import pandas as pd
from pprint import pformat
from zipfile import ZipFile

class AWSM_Geoserver(object):
    def __init__(self, fname, log=None, debug=False, bypass=False, cleanup=True):

        # Setup external logging if need be
        if log==None:
            self.log = logging.getLogger(__name__)
        else:
            self.log = log

        if debug:
            self.debug = debug
            level='DEBUG'
        else:
            self.debug = False
            level="INFO"

        self.cleanup = cleanup

        # Assign some colors and formats
        coloredlogs.install(fmt='%(levelname)-5s %(message)s', level=level,
                                                               logger=self.log)
        self.log.info("\n================================================\n"
                        " Geoserver Upload/Download Script (GUDS) v{}\n"
                        "================================================\n"
                      "".format(__version__))

        with open(fname) as fp:
            cred = json.load(fp)
            fp.close()

        # Geoserver credentials
        self.geoserver_password = cred['geoserver_password']
        self.geoserver_username = cred['geoserver_username']

        # setup the URL
        self.url = cred['url']
        if self.url[-1] != '/':
            self.url +='/'
        self.url = urljoin(self.url,'rest/')

        # Bypass set to true will answer yes to all yes/no questions
        self.bypass = bypass

        # Extract the base url
        self.base_url = urlparse(self.url).netloc

        # Handle IP addresses and ports
        if ":" in self.base_url:
            self.base_url = "".join(self.base_url.split(":")[0:-1])

        self.credential = (self.geoserver_username, self.geoserver_password)

        if 'pem' in cred.keys():
            self.pem = cred['pem']

        self.data = cred['data']

        # Names we want to remap
        self.remap = {'snow_density':'density',
                      'specific_mass':'SWE',
                      'thickness':'depth'}

        # Auto assign layers to colormaps
        self.colormaps_keys = ["depth", "density","swe", "dem", "cold_content",
                            "veg","height", "mask", "basin", "subbasin"]
        # temporary directory
        self.tmp = 'tmp'

        # Make a temporary folder for files
        if not os.path.isdir(self.tmp):
            os.mkdir(self.tmp)

        # A location to store image ranges
        self.ranges = {}

        # Some basin info
        self.log.info("URL:{}".format(self.url))
        self.log.debug("Base URL: {}".format(self.base_url))

    def post(self, resource, payload):
        """
        Wrapper for post request.

        Args:
            resource: Relative location from the http root
            payload: Dictionary containing data to transfer.

        Returns:
            string: request status
        """

        headers = {'content-type' : 'application/json'}
        request_url = urljoin(self.url, resource)
        self.log.debug("POST request to {}".format(request_url))
        r = requests.post(
            request_url,
            headers=headers,
            json=payload,
            verify=True,
            auth=self.credential
        )

        result = r.raise_for_status()

        self.handle_status(resource,r.status_code)

        self.log.debug("POST request returns {}:".format(result))
        return result

    def make(self, resource, payload):
        """
        Wrapper for post request.

        Args:
            resource: Relative location from the http root
            payload: Dictionary containing data to transfer.

        Returns:
            string: request status
        """

        headers = {'content-type' : 'application/json'}
        request_url = urljoin(self.url, resource)
        self.log.debug("POST request to {}".format(request_url))
        r = requests.post(
            request_url,
            headers=headers,
            data=json.dumps(payload),
            verify=True,
            auth=self.credential
        )

        result = r.raise_for_status()

        self.handle_status(resource,r.status_code)

        self.log.debug("POST/MAKE request returns {}:".format(result))
        return result

    def delete(self, resource, **kwargs):
        """
        Wrapper for delete request.

        Args:
            resource: Relative location from the http root
            kwargs: Any pass through items that the request will take

        Returns:
            string: request status
        """

        headers = {'content-type':'application/json'}
        request_url = urljoin(self.url, resource)
        self.log.debug("DELETE request to {}".format(request_url))

        r = requests.delete(
            request_url,
            headers=headers,
            verify=True,
            auth=self.credential,
            params=kwargs
        )

        self.handle_status(resource, r.status_code)

        self.log.debug("Response from DELETE: {}".format(r))

        return r.raise_for_status()

    def move(self, resource, fname, data_type="style", stream=False):
        """
        Wrapper for the put function in the request library, this is written
        to move files from loca to the geoserver
        """
        if data_type =="style":
            headers = {'accept':'application/vnd.ogc.sld+xml',
                       'content-type': 'application/vnd.ogc.sld+xml'}
            mode = 'r'

        elif data_type == 'shapefile':
             headers = {"Content-Type": "application/zip"}
             mode = 'rb'

        else:
            headers = {"accept":'application/octet-stream',
                       "content-type": "application/octet-stream"}
            mode = 'rb'
        request_url = urljoin(self.url, resource)

        self.log.debug("PUT/MOVE request to {}".format(request_url))

        with open(fname, mode) as fp:
            r = requests.put(
                request_url,
                headers=headers,
                data=fp,
                auth=self.credential,
                allow_redirects=True)

            fp.close()

        self.handle_status(resource, r.status_code)

        self.log.debug("Response from PUT: {}".format(r))

        return r.raise_for_status()

    def handle_status(self, resource, code):
        """
        Handles logging code
        """
        msg = "Resource {}".format(resource)
        self.log.debug("Status Code Recieved: {}".format(code))

        if code == 404:
            self.log.error(msg + " was not found on geoserver.".format(resource))
            sys.exit()

        elif code == 200:
            self.log.debug(msg + " was found successfully!")

        elif code == 201:
            self.log.debug(msg + " was created successfully!")

        elif code == 302:
            self.log.debug(msg + " was redirected.")


    def get(self, resource, headers = {'Accept':'application/json'}, skip_json=False):
        """
        Wrapper for requests.get function.
        Retrieves info from the resource and returns the dictionary from the
        json

        Args:
            resource: Relative location from the http root

        Returns:
            dict: Dictionary containing infor about the resource
        """

        request_url = urljoin(self.url, resource)
        self.log.debug("GET request to {}".format(request_url))

        r = requests.get(
            request_url,
            verify=True,
            headers=headers,
            auth=self.credential
        )

        if not skip_json:
            self.handle_status(resource, r.status_code)
            result = r.json()
        else:
            result = r

        self.log.debug("GET Returns: {}".format(pformat(result)))

        return result

    def put(self, resource, payload, headers = {'Accept':'application/json', "Content-Type":"application/json"}):
        """
        Wrapper for requests.put function.
        puts info into the resource and returns the dictionary from the
        json

        Args:
            resource: Relative location from the http root
            payload: Json data
        Returns:
            status code
        """

        request_url = urljoin(self.url, resource)
        self.log.debug("PUT request to {}".format(request_url))

        r = requests.put(
            request_url,
            headers=headers,
            json=payload,
            verify=True,
            auth=self.credential,
            allow_redirects=True
        )

        result = r.raise_for_status()

        self.handle_status(resource,r.status_code)

        self.log.debug("PUT request returns {}:".format(result))
        return result

    def grab(self, resource, fname):
        """
        Wrapper for requests.get function.
        Retrieves data from the resource and writes a file

        Args:
            resource: Relative location from the http root
            fname: Name of the file to save
        """

        request_url = urljoin(self.url, resource)

        self.log.debug("GET/GRAB request to {}".format(request_url))

        r = requests.get(
            request_url,
            stream=True,
            verify=True,
            auth=self.credential,
            allow_redirects=True
        )
# /geoserver/rest/resource/data/basins/kings/masked_snow_20180418.nc
#
# /geoserver/rest/resource/data/basins/kings/masked_snow_20190418.nc


        self.handle_status(resource,r.status_code)

        self.log.info("Writing data to {} ...".format(fname))
        with open(fname,"wb") as fp:
            for chunk in r.iter_content(chunk_size=1024):
                 # writing one chunk at a time to pdf file
                 if chunk:
                     fp.write(chunk)

        self.log.info("File Downloaded to {}".format(fname))

    def get_basins(self):
        """
        Retrieves all the workspaces/ basins and returns a list of names
        """
        rjson = self.get("workspaces/")
        basins = []

        if rjson["workspaces"]:
            for b in rjson["workspaces"]["workspace"]:
                basins.append(b["name"])
        else:
            self.log.warn("No basins found!")

        return basins

    def get_coverages(self, basin):
        """
        Returns a list of names currently on the geoserver for a given basin
        """
        coverageStores = []
        rjson = self.get("workspaces/{}/coveragestores".format(basin))

        # If there is a list
        if rjson["coverageStores"]:
            for cs in rjson["coverageStores"]["coverageStore"]:
                coverageStores.append(cs["name"])
        else:
            self.log.warn("No coverages found for the {}".format(basin))

        return coverageStores

    def get_layers(self, basin):
        """
        Returns a list of names currently on the geoserver for a given basin
        """

        layers = []
        rjson = self.get("workspaces/{}/layers".format(basin))

        # If there is a list
        if rjson["layers"]:
            for lyr in rjson["layers"]["layer"]:
                layers.append(lyr["name"])
        else:
            self.log.warn("No layers found for the {}".format(basin))

        return layers

    def extract_data(self, fname, upload_type='modeled', espg=None, mask=None):
        """
        Args:
            fname: String path to a local file.
            upload_type: specifies whether to name a file differently
            espg: Projection code to use if projection information not found if
                  none, user will be prompted

        Returns:
            fname: New name of file where data was extracted.
        """

        # Check for netcdfs
        if fname.split('.')[-1] == 'nc':
            # AWSM related items should have a variable called projection
            ds = Dataset(fname, 'r')

            # Base file name
            bname = os.path.basename(fname)

            if upload_type=='modeled':

                # Add a parsed date to the string to avoid overwriting snow.nc
                self.log.info("Retrieving date from netcdf...")
                time = ds.variables['time']
                dates = num2date(time[:], units=time.units,
                                          calendar=time.calendar)
                self.date = dates[0].isoformat().split('T')[0]

                cleaned_date = "".join([c for c in self.date if c not in ':-'])
                bname = bname.split(".")[0] + "_{}.nc".format(cleaned_date)
                fname = bname

                # Only copy some of the variables
                if "cold_content" in ds.variables.keys():
                    self.log.info("Uploading energetics file, only uploading "
                                  "cold_content")
                    keep_vars = ['x','y','time','projection','cold_content']
                else:
                    self.log.info("Uploading snow state file, only uploading "
                                  "specific_mass, thickness, and snow_density")
                    keep_vars = ['x','y','time','projection', 'snow_density',
                                                              'specific_mass',
                                                              'thickness']

                exclude_vars = [v for v in ds.variables.keys() \
                                if v not in keep_vars]
                mask_exlcude = []

            elif upload_type=='topo':
                self.date = dt.today().isoformat().split('T')[0]
                mask = fname
                bname = bname.split(".")[0] + "_{}.nc".format(self.date)
                fname = bname
                mask_exlcude = ['mask']
                keep_vars = ds.variables.keys()

            fname = os.path.join(self.tmp, fname)
            exclude_vars = [v for v in ds.variables.keys() \
                            if v not in keep_vars]

            # Create a copy
            self.log.info("Copying netcdf...")
            new_ds = copy_nc(ds, fname, exclude = exclude_vars)

            # Calculate mins and maxes
            for lyr in [l for l in keep_vars if l not in ['x','y','time','projection']]:
                self.ranges[lyr] = [np.nanmin(new_ds.variables[lyr][:]),
                                    np.nanmax(new_ds.variables[lyr][:])]

            # Optional Masking
            if mask != None:
                self.log.info("Masking netcdf using {}".format(mask))
                new_ds.close() # close the last one
                new_ds = mask_nc(fname, mask, exclude=mask_exlcude,
                                              output=self.tmp)
                fname = new_ds.filepath()


            # Check for missing projection
            if 'projection' not in new_ds.variables:
                self.log.info("Netcdf is missing projection information...")

                # Missing ESPG from args
                if espg == None:
                    espg = input("No projection detected. Enter the ESPG code"
                                 " for the data:\n")

                self.log.info("Adding projection information using ESPG code "
                              "{}...".format(espg))
                new_ds = add_proj(new_ds, espg)

            # Clean up
            new_ds.close()
            ds.close()

        return fname

    def copy_data(self, fname, basin):
        """
        Data for the geoserver has to be in the host location for this. We

        Copies data from users location to geoserver/data/<basin>/

        Args:
            fname: String path to a local file.
            basin: String name of the targeted basin/workspace to put the file
                   in

        Returns:
            final_fname: The remote path to the file we copied
        """
        bname = os.path.basename(fname)
        resource = "{}/{}/{}".format(self.data, basin, bname)

        self.log.info("Copying local data to remote, this may take a couple "
                      "minutes...")

        self.move(resource, fname, data_type="modeled", stream=True)
        self.log.debug("data sent to : {}".format(fname))

        # Geoserver paths don't see the resource folder.
        final_fname = "{}/{}/{}".format(os.path.basename(self.data), basin, bname)
        self.log.debug("File path for the geoserver is: {}".format(final_fname))

        return final_fname

    def exists(self, basin, store=None, dstore=None, layer=None):
        """
        Checks the geoserver if the object exist already by name. If basin
        store and layer are provided it will check all three and only return
        true if all 3 exist.

        Args:
            basin: String name of the targeted, this script assumes the basin
                   name and workspace are the same.
            store: String name of the data/coverage storage object.
            dstore: string name of the data store
            layer: String name of the layer

        Returns:
            bool: True if all non-None values of the basin,store,layer exists,
                  False otherwise
        """

        store_exists = None
        layer_exists = None
        dstore_exists = None

        # We always will check for the basins existence
        ws_exists = False

        # Does the workspace > coveragetore/datastore exist
        if store != None:
            store_exists = False
            if dstore != None:
                raise ValueError(" Cannot check for coverage and data stores at"
                                " the same time")

        # Check for data stores
        elif dstore != None:
            dstore_exists = False

        # Does the workspace > coverage/datastore > layer exist
        if layer != None:
            layer_exists = False

        rjson = self.get('workspaces')

        # Are there any workspaces?
        if rjson['workspaces']:
            ws_info = rjson['workspaces']

            # Check if the basin exists as a workspace
            for w in ws_info['workspace']:
                if basin.lower() == w['name']:
                    ws_exists = True
                    break

            # coverageStore existence requested
            if store != None:
                # Grab info about this existing workspace
                ws_dict = self.get(w['href'])

                # Grab info on any coverage stores
                cs_dict = self.get(ws_dict['workspace']['coverageStores'])

                # Check if there are any coverage stores
                if cs_dict['coverageStores']:
                    cs_info = cs_dict['coverageStores']

                    # Check for matching name in the coverages
                    for cs in cs_info['coverageStore']:
                        if store == cs['name']:
                            store_exists = True
                            break

            # Check if there are any datastores
            elif dstore != None:
                # Grab info about this existing workspace
                ws_dict = self.get(w['href'])
                ds_dict = self.get(ws_dict['workspace']['dataStores'])

                if ds_dict["dataStores"]:
                    ds_info = ds_dict['dataStores']

                    # Check for matching name in the coverages
                    for ds in ds_info['dataStore']:
                        if dstore == ds['name']:
                            dstore_exists = True
                            break


            # layer existence requested
            if layer != None and store_exists:
                # Grab info about this existing store
                store_info = self.get(cs['href'])
                coverages = self.get(store_info['coverageStore']['coverages'])

                # Check to see if there any coverages at all
                if coverages['coverages']:
                    for cv in coverages['coverages']['coverage']:
                        if layer == cv['name']:
                            layer_exists = True

            # layer existence requested
        elif layer != None and dstore_exists:
                # Grab info about this existing store
                store_info = self.get(ds['href'])
                vectors = self.get(store_info['coverageStore']['coverages'])

                # Check to see if there any datastores at all
                if vectors['coverages']:
                    for v in vectors['coverages']['coverage']:
                        if layer == v['name']:
                            layer_exists = True


        result = [ws_exists, store_exists, dstore_exists, layer_exists]
        expected = [r for r in result if r != None]
        truth = [r for r in result if r == True]

        msg = " > ".join([r for r in [basin, store, dstore, layer] if r !=None])

        if len(truth) == len(expected):
            self.log.debug("{} already exists on the geoserver.".format(msg))
            return True
        else:
            self.log.debug("{} doesn't exist on the geoserver.".format(msg))
            return False

    def create_basin(self, basin):
        """
        Creates a new basin on the geoserver. Important to note that this script
        treats the names of workspaces as the same name as the basin.

        Args:
            basin: String name of the new basin/workspace
        """

        create_ws = ask_user("You are about to create a new basin on the"
                             " geoserver called: {}\nAre you sure you want"
                             " to continue?".format(basin), bypass=self.bypass)

        if not create_ws:
            self.log.info("Aborting creating a new basin. Exiting...")
            sys.exit()

        else:
            self.log.info("Creating new basin {} on geoserver...".format(basin))
            payload = {'workspace': {'name':basin,
                                     'enabled':True}}

            rjson = self.make('workspaces', payload)

    def create_coveragestore(self, basin, store, filename, description=None,
                                                           store_type="NetCDF"):
        """
        Creates a coverage data store for raster type data on the geoserver.

        Args:
            basin: String name of the targeted basin/workspace
            store: String name of the new coverage data store
            filename: to a netcdf/geotiff on the geoserver
            description: text to include with the file
            store_type: Geotiff or Netcdf for coverage

        """

        bname = os.path.basename(filename)

        # Check to see if the store already exists...
        if self.exists(basin, store=store):

            self.log.warn("Coverage store {} exists!".format(store))

            # Check to see if user wants to delete it and rewrite it
            ans = ask_user("Do you want to overwrite coveragestore {}?"
                           "".format(store), bypass=self.bypass)

            if ans:
                resource = "workspaces/{}/coveragestores/{}.json".format(basin,
                                                                         store)
                self.delete(resource, recurse=True, purge=True)

            else:
                self.log.info("Unable to continue, exiting...")
                sys.exit()

        # Make the coverage store!
        resource = 'workspaces/{}/coveragestores.json'.format(basin)

        payload = {"coverageStore":{"name":store,
                                    "type":store_type,
                                    "enabled":True,
                                    "_default":False,
                                    "workspace":{"name": basin},
                                    "configure":"all",
                                    "url":"file:{}".format(filename)}}
        if description != None:
            payload['coverageStore']["description"] = description

        create_cs = ask_user("You are about to create a new geoserver"
                             " coverage store called: {} in the {}\nAre "
                             " you sure you want to continue?"
                             "".format(store, basin), bypass=self.bypass)
        if not create_cs:
            self.log.info("Aborting creating a new coverage store."
                          "Exiting...")
            sys.exit()
        else:
            self.log.info("Creating a new coverage on geoserver...")
            self.log.debug(pformat(payload))
            rjson = self.make(resource, payload)

    def create_latest_layers(self, basin):
        """
        Creates a 3 new layers call latest_<variable>
        for a given basin. Calculates all the layers dates and finds the most
        recent one and makes a copy of it associated to lates_<variable>

        """

        self.log.info("Determining the date for latest variables...")
        coverages = self.get_coverages(basin)
        dates = []

        # Get all the coverage names/ check dates avoiding latest
        for cs in coverages:
            var_nm_date = cs.split(':')[-1].lower()
            if "latest" not in var_nm_date:
                sdate = "".join([s for s in var_nm_date if s.isnumeric()])
                dates.append(pd.to_datetime(sdate))

        # Find the most recent modeling date
        latest_date = max(dates)
        latest_str = "".join(latest_date.isoformat().split('T')[0].split("-"))


        self.log.info("Using {} for the latest model date..."
                      "".format(latest_date.isoformat().split('T')[0]))

        # Grab all the coverages with the most recent date in their name
        latest_coverages = [cs for cs in coverages if latest_str in cs]

        # Get the coverage stor info
        resource = "workspaces/{}/coveragestores/{}".format(basin,
                                                            latest_coverages[0])
        cs_info = self.get(resource)

        # Modify the info's name
        name_o = cs_info['coverageStore']["name"]
        name = self.get_latest_name(name_o)

        # Remove any trailing underscores
        if name[-1] == "_":
            name = name[0:-1]

        cs_info["coverageStore"]["name"] = name

        # Check to see if the store already exists
        if self.exists(basin, store=name):
            resource = "workspaces/{}/coveragestores/{}".format(basin, name)
            self.delete(resource,  purge=True, recurse=True)

        # Create a copy of the store under a new name
        self.log.info("Creating new store called {} using a store called {}"
                      "".format(name, name_o))
        resource = "workspaces/{}/coveragestores/".format(basin)
        self.make(resource, cs_info)

        # Copy existing coverages
        resource = "workspaces/{}/coveragestores/{}/coverages".format(basin,
                                                                      name_o)
        coverages = self.get(resource)

        # resource to post to
        resource = "workspaces/{}/coveragestores/{}/coverages".format(basin,
                                                                      name)

        if coverages['coverages']:
            for c in coverages['coverages']['coverage']:
                cov_info = self.get(c['href'])["coverage"]

                cov_name = self.get_latest_name(cov_info['name'])
                self.log.info("Copying coverage info from {} to {}".format(
                                                                    cov_info["name"],
                                                                    cov_name))

                # Modify the original payload
                cov_info['name'] = cov_name
                cov_info['store'] = {"name":"{}:{}".format(basin, name)}
                self.make(resource, {"coverage":cov_info})
                self.assign_colormaps(basin, cov_name)

        else:
            self.log.error("No layers associated to store {} to copy for latest"
                           "".format(name_o))

    def create_layer(self, basin, store, layer):
        """
        Create a raster layer on the geoserver

        Args:
            basin: String name of the targeted basin/workspace
            store: String name of the targeted data/coverage store
            layer: String name of the new layer to be made

        """
        resource = ("workspaces/{}/coveragestores/{}/coverages.json"
                   "".format(basin, store))

        lyr_name = layer.replace(" ","_").replace('-','')
        native_name = lyr_name #layer.replace('_',' ')

        # Make the names better/ Rename the isnobal stuff
        if native_name in self.remap.keys():
            name = self.remap[native_name]
        else:
            name = lyr_name

        # Human readable title for geoserver UI
        lname = name.lower()
        if lname == 'swe':
            title = ("{} {} {}".format(basin.title(),
                                       self.date,
                                       name.upper())).replace("_"," ")
        elif "super" in lname:
            title = ("{} {} Lidar Flight".format(basin.title(), self.date))

        else:
            title = ("{} {} {}".format(basin,
                                       self.date,
                                       name)).replace("_"," ").title()

        # Add an associated date to the layer
        if hasattr(self,'date'):
            name = "{}{}".format(name, self.date.replace('-',''))

        payload = {"coverage":{"name":name,
                               "nativeName":lyr_name,
                               "nativeCoverageName":native_name,
                               "store":{"name": "{}:{}".format(basin, store)},
                               "enabled":True,
                               "title":title
                               }}

        # If we have ranges for the layer, use it.
        if lyr_name in self.ranges.keys():
            self.log.info("Setting range for {} to {}..."
                          "".format(lyr_name, self.ranges[lyr_name]))

            payload["coverage"]["dimensions"] = {"coverageDimension":[
                        {"name":"{}".format(name),
                         "range":{"min":"{}".format(self.ranges[lyr_name][0]),
                                  "max":"{}".format(self.ranges[lyr_name][1])},
                          }]
                                                }
        # submit the payload for creating a new coverage
        self.log.debug("Payload: {}".format(payload))

        # Check if it exists first
        if self.exists(basin, store=store, layer=name):
            self.delete("workspaces/{}/coveragestores/{}/coverages/{}"
                        "".format(basin, store, layer), purge=True,
                                                        recursive=True)

        response = self.make(resource, payload)

        # Assign Colormaps
        self.assign_colormaps(basin, name)

    def assign_colormaps(self, basin, name, layer_type="raster"):
        """
        currently utilizes a hacky version to accomplish our goal. function
        Assigns the colormaps to default and styles available

        Args:
            basin: name of the basin
            name: name of the layer
            layer_type: raster or vector to identify how we assign defaults
        """
        # All colormaps we want to assign
        colormaps = self.get_keyword_styles(name)

        # Default colormap
        if layer_type=='raster':
            colormaps.append("raster")

            if "dynamic_default" in colormaps:
                colormaps.append("dynamic_default")

        # Add all the colormaps on at a time
        styles_list = [{"name":c} for c in colormaps]
        resource = "layers/{}:{}/styles.json".format(basin, name)
        for c in colormaps:
            self.log.info("Adding style {} to {}:{}".format(c, basin, name))
            payload = {"style":{"name":c}}
            self.post(resource, payload)

        # Currently Erases all my added styles when I attempt to add default
        # self.log.info("Default style for {}:{} set to {}".format(basin, name,
        #                 colormaps[-1]))
        # Set the default colormap which should always be the last one
        # styles = self.get(resource)["styles"]
        # default = [c for c in styles["style"] if c["name"]==colormaps[-1]][0]
        # payload = {"layer":{"defaultStyle":default}}
        #
        # resource = "layers/{}:{}.json".format(basin, name)
        # r = self.put(resource, payload)


    def get_keyword_styles(self, layer_name):
        """
        Returns all the styles that has keywords matching in the layer_name
        and in the style name for rasters only

        Args:
            layer_name: Name of the layer being made
        """

        styles = self.get('styles/')
        avail = [k['name'] for  k in styles['styles']['style']]
        result = []

        # Filter the styles
        for key in self.colormaps_keys:
            for style in avail:
                if key in style.lower() and key in layer_name.lower():
                    result.append(style)

        self.log.info("{}/{} availables styles are matching".format(len(result),
                                                                    len(avail)))

        # Add in dynamic_default default if it is there
        if "dynamic_default" in avail:
            result.append('dynamic_default')

        return list(set(result))

    def create_layers_from_netcdf(self, basin, store, filename, layers=None,):
        """
        Opens a netcdf locally and adds all layers to the geoserver that are in
        the entire image if layers = None otherwise adds only the layers listed.

        Args:
            basin: String name of the targeted basin/workspace
            store: String name of a targeted netcdf coverage store
            layers: List of layers to add, if none add all layers except x,y,
                    time, and projection
        """

        for name in layers:

            if self.exists(basin, store=store, layer=name):
                self.log.info("Layer {} from store {} in the {} exists..."
                      "".format(name, store, basin))
                self.log.warning("Skipping layer {} to geoserver.".format(name))
            else:
                self.log.info("Adding {} from {} to the {}".format(name,
                                                           store,
                                                           basin))
                self.create_layer(basin, store, name)

    def upload(self, basin, filename, upload_type='modeled', espg=None,
                                                             mask=None):
        """
        Generic upload function to redirect to specific uploading of special
        data types, under development, currently only topo images work. Requires
        a local filepath which is then uploaded to the geoserver.

        Args:
            basin: string name of the basin/workspace to upload to.
            filename: path of a local to the script file to upload
            upload_type: Determines how the data is uploaded
            mask: Filename of a netcdf containing a mask layer
        """
        self.log.info("Associated Basin: {}".format(basin))
        self.log.info("Data Upload Type: {}".format(upload_type))
        self.log.info("Source Filename: {}".format(filename))
        self.log.info("Mask Filename: {}".format(mask))

        if not os.path.isfile(filename):
            self.log.error("Upload file doesn't exist.")
            sys.exit()

        if mask != None:
            if not os.path.isfile(mask):
                self.log.error("Mask file doesn't exist.")
                sys.exit()

        # Ensure that this workspace exists
        if not self.exists(basin):
            self.create_basin(basin)

        # Handle netcdfs
        if upload_type in ['topo','modeled']:
            # Reduce the size of netcdfs if possible return the new filename
            filename = self.extract_data(filename, upload_type=upload_type,
                                                   espg=espg,
                                                   mask=mask)

            # Grab the layer names
            ds = Dataset(filename)

            layers = []
            for name, v in ds.variables.items():
                if name not in ['time','x','y','projection']:
                    layers.append(name)

            if len(layers) == 0:
                self.log.error("No variables found in netcdf...exiting.")
                sys.exit()

        # Copy users data up to the remote location
        remote_fname = self.copy_data(filename, basin)

        # Check for the upload type which determines the filename, and store
        if upload_type == 'topo':
            self.submit_topo(filename, remote_fname, basin, layers=layers)

        elif upload_type == 'modeled':
            self.submit_modeled(filename, remote_fname, basin, layers=layers)

        elif upload_type == 'flight':
            self.submit_flight(remote_fname, basin)

        elif upload_type == 'shapefile':
            self.submit_shapefile(filename, basin)

        elif upload_type == 'png':
            pass

        else:
            raise ValueError("Invalid or undeveloped upload type requested!")

        # Cleanup
        if self.cleanup:
            self.log.info("Cleaning up files... Removing {}".format(self.tmp))
            rmtree(self.tmp)

    def submit_topo(self, filename, remote_filename, basin, layers=None):
        """
        Uploads the basins topo images which are static. These images include:
        * dem
        * basin mask
        * subbasin masks
        * vegetation images relating to types, albedo, and heights

        Args:
            filename: Remote path of a netcdf to upload
            basin: Basin associated to the topo image
            layers: Netcdf variables names to add as layers on GS
        """

        # Always call store names the same thing, <basin>_topo
        store_name = "{}_topo".format(basin)
        description = ("NetCDF file containing topographic images required for"
                       " modeling the {} watershed in AWSM.\n"
                       "Uploaded: {}").format(basin, self.date)

        self.create_coveragestore(basin, store_name, remote_filename,
                                                     description=description)

        self.create_layers_from_netcdf(basin, store_name, filename,
                                                          layers=layers)

    def submit_modeled(self, filename, remote_filename, basin, layers=None):
        """
        Uploads the basins modeled data. These images include:
        * density
        * specific_mass
        * depth

        Args:
            filename: Remote path of a netcdf to upload
            basin: Basin associated to the topo image
            layers: Netcdf variables names to add as layers on GS

        """

        # Always call store names the same thing, <basin>_snow_<date>
        store_name = "{}_{}".format(basin,
                                    os.path.basename(filename).split(".")[0])
        # Create Netcdf store
        description = ("NetCDF file containing modeled snowpack images from "
                       "the {} watershed produced by AWSM.\n"
                       "Model Date: {}\n"
                       "Date Uploaded: {}").format(basin,
                                       self.date,
                                       dt.today().isoformat().split('T')[0])

        self.create_coveragestore(basin, store_name, remote_filename,
                                                     description=description)

        # Create layers density, specific mass, thickness
        self.create_layers_from_netcdf(basin, store_name, filename,
                                                          layers=layers)

    def submit_shapefile(self, filename, basin, layer=None):
        """
        Uploads the shapefiles. If layer=None then it simply uses the filename
        to create the layer name (replacing underscores with spaces)

        Args:
            filename: Local path to a .shp file
            basin: string name of the workspace or basin
            layer: Alternate layer name to use

        """
        filename = os.path.abspath(filename)
        bname = os.path.basename(filename)
        keyword = bname.split('.')[0]
        dstore = keyword + "_store"

        # Get all the files associated with the shapefile
        associate_files = os.listdir(os.path.dirname(filename))
        associate_files = [f for f in associate_files if keyword in f]

        resource = "workspaces/{}/datastores".format(basin)
        if self.exists(basin, dstore=dstore):
           resource = resource + "/" + dstore
           self.delete(resource, purge=True, recurse=True)

        # Create a new store
        payload = {"dataStore": {
                                "name": dstore,
                                "connectionParameters":
                                    {"entry":
                                        [{"@key":"url","$":"file:basins/{}/{}"
                                                   "".format(basin, bname)}]

                                    }
                                }
                    }

        resource = "workspaces/{}/datastores".format(basin)
        self.make(resource, payload)

        # Upload the related files
        self.log.info("Uploading {} files.".format(len(associate_files)))

        for f in associate_files:
            self.log.debug("Uploading {}".format(f))
            bname = os.path.basename(f)
            fresource = "resource/basins/{}/{}".format(basin, bname)

            r = self.get(fresource, skip_json=True)

            if r.status_code == 200:
                self.delete(fresource, purge=True, recurse=True)

            # Upload each file
            self.move(fresource, f, data_type="shapefile")

        # Create the layer
        resource = "workspaces/{}/datastores/{}/featuretypes".format(basin,
                                                                     dstore)

        payload = {"featureType":{"name":keyword,
                            "title":keyword.replace("_"," ").title(),
                            "store":{"name":"{}:{}".format(basin, dstore)}
                                 }
                  }

        self.make(resource, payload)
        self.assign_colormaps(basin, keyword, layer_type="vector")

    def download(self, basin, date_str, download_type="modeled"):
        """
        Downloads data
        Args:
            basin: String name of the basin.
            date_str: String date of the file you want to download
        """
        date = pd.to_datetime(date_str)
        date_str = "".join(date.isoformat().split('T')[0].split("-"))

        if download_type == "modeled":
            fname = "masked_snow_{}.nc".format(date_str)

        else:
            self.log.error("{} data downloads have not been develop yet!")
            sys.exit()

        self.log.info("Download Requested. Attempting to download {} from the "
                      "{}.".format(fname, basin))

        resource = "{}/{}/{}".format(self.data, basin,fname)
        self.grab(resource, fname)

    def submit_styles(self, local_files, basin=None):
        """
        Uses a post to make the styles available, then uses a put to actually
        move the styles information there. Then go through and run submit_styles
        over all of the available layers

        Args:
            local_files: List of styles to upload. Must be SLD.
            basin: Specify a basin who's layers will receive the new styles,
                 default is none which will apply them to all

        """
        resource = "styles/"
        existing_styles = self.get(resource)
        existing_styles = [style['name'] for style in \
                                             existing_styles['styles']['style']]
        self.log.info("Uploading {} styles.".format(len(local_files)))
        self.log.info("{} styles already exist.".format(len(existing_styles)))

        # Loop through all the SLD files to upload
        for f in local_files:
            skip = False

            style_name = os.path.basename(f).split('.')[0]
            style_resource = "styles/{}".format(style_name)

            # Check if this is already exists.
            if style_name in existing_styles:
                ans = ask_user("You are about to overwrite the style {}."
                         "\nDo you want to continue?".format(style_name),
                         bypass=self.bypass)
                # Overwrite
                if ans:
                    self.delete(style_resource, purge=True, recurse=True)

                # Skip it
                else:
                    self.log.warn("Skipped overwriting {}!".format(style_name))
                    skip = True

            # Upload that bad boy
            if not skip:
                self.log.info("Adding the {} style to the geoserver..."
                                                          "".format(style_name))
                payload = {"style":{"name":style_name,
                                    "filename":os.path.basename(f)}}
                resource = "styles/"

                # Create the placeholder
                self.make(resource, payload)

                # Move the SLD content up
                self.move(style_resource, f)

        # Go back and update all the layers styles
        if basin != None:
            basins = [basin]
        else:
            basins = self.get_basins()

        for b in basins:
            layers = self.get_layers(b)

            # Go back and assign colormaps only to the ones that we mess with
            for f in local_files:
                keys = [k for k in self.colormaps_keys if k in f]

            final_layers = []
            for lyr in layers:
                if len([True for k in keys if k in lyr]) > 0:
                    final_layers.append(lyr)

            self.log.info("Assigning style to {} layers on the {}"
                          "".format(len(final_layers), b))
            for lyr in final_layers:
                self.assign_colormaps(b, lyr)

    def submit_flight(self, filename, basin):
        """
        Uploads an ASO 3m lidar overpass. Date should be in the filename such as
        USCALB20190325_SUPERsnow_depth.tif is for the lakes basin on 2019-03-25

        Args:
            filename: Remote name of the file
            basin: basin the file is associated with
        """

        # Naming
        bname = os.path.basename(filename).split('.')[0]
        store = bname + "_store"

        # Dates
        no_format_date = "".join([s for s in store if s.isnumeric()])
        self.date = pd.to_datetime(no_format_date).date().isoformat()

        self.log.info("Identified flight date as: {}".format(self.date))
        description = ("ASO 3 meter lidar over pass for the {} basin on {}"
                      "".format(basin, self.date))

        # Create the store which also creates the layer
        self.create_coveragestore(basin, store, filename,
                                                description=description,
                                                store_type='GeoTIFF')

        self.create_layer(basin, store, bname)

    def get_latest_name(self, name_o):
        """
        Takes the original name and removes the date, then adds latest to the
        name. To be used when assigning copy layers to the latest raster
        """
        name = "".join([s for s in name_o if not s.isnumeric()])
        name = "latest_{}".format(name)

        if name[-1] == "_":
            name = name[0:-1]

        return name


def ask_user(msg, bypass=False):
    """
    Asks the user yes no questions

    Args:
        msg: question to display
        bypass: Handle passing yes always
    Returns:
        response: boolean indicating whether to proceed or not.
    """

    acceptable = False

    # Wait for a recognizeable answer
    while not acceptable:
        if bypass:
            ans='yes'

        else:
            ans = input(msg+' (y/n)\n')

        if ans.lower() in ['y','yes']:
            acceptable = True
            response = True

        elif ans.lower() in ['n','no']:
            acceptable = True
            response = False
        else:
            print("Unrecognized answer, please use (y, yes, n, no)")

    return response

def write_json(bypass=False):
    """
    Writes a blank json with all the keys required to run the script
    """
    fname = "./geoserver.json"
    ans = False

    # Ask user to overwrite
    if os.path.isfile(fname):
        ans = ask_user("You are about to overwrite an existing file to write"
                       " your credentials json, do you want to continue?",
                       bypass=bypass)
        if not ans:
            sys.exit()

    with open(fname, 'w') as fp:
        line = \
        ('{"url":"",\n"geoserverusername":"",\n'
        '"geoserver_password":"",\n"data":""}\n')
        fp.write(line)
        fp.close()


def main():
    # Parge command line arguments
    p = argparse.ArgumentParser(description="Submits either a lidar flight,"
                                            " AWSM/SMRF topo image, or AWSM "
                                            " modeling results to a geoserver")

    p.add_argument('-f','--files', dest='filenames', nargs='+',
                    help="Path(s) to a file containing either a lidar flight,"
                    "AWSM/SMRF topo image, AWSM modeling snow.nc, shapefiles"
                    " or a list of styles")

    p.add_argument('-b','--basin', dest='basin',
                    choices=['brb', 'kaweah', 'kings', 'lakes', 'merced',
                             'sanjoaquin','tuolumne','gunnison'], required=False,
                    help="Basin name to submit to which is also the geoserver"
                         " workspace name")

    p.add_argument('-c','--credentials', dest='credentials',
                    default='./geoserver.json',
                    required=False,
                    help="JSON containing geoserver credentials for logging in")

    p.add_argument('-t','--data_type', dest='data_type',
                    default='modeled',
                    choices=['flight','topo','shapefile','modeled','styles','png'],
                    required=False,
                    help="Data type dictates how some items are "
                         "uploaded/downloaded.")

    p.add_argument('-e','--espg', dest='espg',
                    type=int, default=None,
                    help="espg value representing the projection information to"
                    "add to the netcdf")

    p.add_argument('-m','--mask', dest='mask',
                    type=str, default=None,
                    help="Netcdf containing a mask layer")

    p.add_argument('--write_json', dest='write_json', action='store_true',
                    help="Creates a blank geoserver.json file to fill out")

    p.add_argument('-d','--debug', dest='debug', action='store_true',
                    help="Creates a blank geoserver.json file to fill out")

    p.add_argument('-y','--bypass', dest='bypass', action='store_true',
                    help="Answers yes to all the questions. It is important to"
                    " not use unless you are very confident you have the"
                    " correct names.")

    p.add_argument('-ncu','--no_cleanup', dest='cleanup', action='store_false',
                    help="When used, it doesn't clean up the files it creates."
                    " Not to be used for other than debugging.")

    p.add_argument('-do','--download', dest='download',
                    help="Receives a date for downloading files")

    p.add_argument('-l','--latest', dest='latest', action="store_true",
                    help="If used guds will also create a latest layer if "
                         "after uploading by looking at all the layers "
                         "available for the associated basin")

    args = p.parse_args()

    # Timing
    start = time.time()

    # User requested a geoserver.json file to fill out.
    if args.write_json:
        write_json(args.bypass)

    else:
        # Get an instance to interact with the geoserver.
        gs = AWSM_Geoserver(args.credentials, debug=args.debug,
                                              bypass=args.bypass,
                                              cleanup=args.cleanup)

        if args.download != None:
            # Download a file
            if args.basin == None:
                gs.log.error("Basin name required for downloading data!")
                sys.exit()

            gs.download(args.basin, args.download, download_type=args.data_type)

        else:
            if args.filenames != None:
                # Submitting styles only
                if args.data_type=="styles":

                    if type(args.filenames)!= list:
                        args.filenames = [args.filenames]

                    gs.submit_styles(args.filenames)

                else:
                    if args.basin == None:
                        gs.log.error("Basin name required for uploading data!")
                        sys.exit()
                    # Upload a file
                    gs.upload(args.basin, args.filenames[0],
                                          upload_type=args.data_type,
                                          espg=args.espg,
                                          mask=args.mask)

        if args.data_type=='modeled' and args.latest:
            gs.create_latest_layers(args.basin)

        # Timing
        end = time.time()
        gs.log.info("Completed in {0:0.1f}s".format(end-start))

if __name__ =='__main__':
    main()
