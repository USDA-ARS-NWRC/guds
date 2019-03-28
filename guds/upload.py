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

        self.geoserver_password = cred['geoserver_password']
        self.geoserver_username = cred['geoserver_username']

        # setup the URL
        self.url = cred['url']
        if self.url[-1] != '/':
            self.url +='/'
        self.url = urljoin(self.url,'rest/')

        self.username = cred['remote_username']
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
        self.colormaps_keys = ["depth","density","swe", "dem",
                            "veg","height","mask"]
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

        self.log.debug("POST request returns {}:".format(result))
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
        self.log.debug("PUT request to {}".format(request_url))

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

    def move(self, resource, fname):
        """
        Wrapper for the put function in the request library, this is written
        to move files from loca to the geoserver
        """
        headers = {'accept':'application/vnd.ogc.sld+xml','content-type': 'application/vnd.ogc.sld+xml'}

        request_url = urljoin(self.url, resource)

        self.log.debug("PUT request to {}".format(request_url))

        with open(fname,'r') as fp:

            r = requests.put(
                request_url,
                headers=headers,
                data=fp,
                auth=self.credential
            )
            fp.close()

        self.handle_status(resource,r.status_code)

        self.log.debug("Response from PUT: {}".format(r))

        return r.raise_for_status()

    def modify(self, resource, payload):
        """
        Wrapper for Put request.

        Args:
            resource: Relative location from the http root
            payload: Dictionary containing data to transfer.

        Returns:
            string: request status
        """
        headers = {'accept':'application/json',
                   'content-type':'application/json'}

        request_url = urljoin(self.url, resource)

        self.log.debug("PUT request to {}".format(request_url))

        r = requests.put(
            request_url,
            headers=headers,
            json=payload,
            auth=self.credential
        )

        self.handle_status(resource,r.status_code)

        self.log.debug("Response from PUT: {}".format(r))

        return r.raise_for_status()

    def handle_status(self, resource, code):
        """
        Handles logging code
        """
        msg = "Resource {}".format(resource)

        if code == 404:
            self.log.error(msg + " was not found on geoserver.".format(resource))
            sys.exit()

        elif code == 200:
            self.log.debug(msg + " was found successfully!")

        elif code == 302:
            self.log.debug(msg + " was redirected.")
        else:
            self.log.debug("Status Code Recieved: {}".format(code))


    def get(self, resource, headers = {'Accept':'application/json'}):
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

        self.handle_status(resource, r.status_code)

        result = r.json()
        self.log.debug("GET Returns: {}".format(pformat(result)))

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
        )

        self.handle_status(resource,r.status_code)

        self.log.info("Writing data...".format(fname))
        with open(fname,"wb") as fp:
            for chunk in r.iter_content(chunk_size=1024):
                 # writing one chunk at a time to pdf file
                 if chunk:
                     fp.write(chunk)

        self.log.info("File Dowload complete. Written to {}".format(fname))

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
                keep_vars = ['x','y','time','snow_density','specific_mass',
                                                           'thickness',
                                                           'projection']

                exclude_vars = [v for v in ds.variables.keys() \
                                if v not in keep_vars]
                mask_exlcude = []

            elif upload_type=='topo':
                self.date = dt.today().isoformat().split('T')[0]

                bname = bname.split(".")[0] + "_{}.nc".format(self.date)
                fname = bname
                mask_exlcude = ['mask']
                keep_vars = ds.variables.keys()
                mask = fname

            fname = os.path.join(self.tmp, fname)
            exclude_vars = [v for v in ds.variables.keys() \
                            if v not in keep_vars]

            # Create a copy
            self.log.info("Copying netcdf...")
            new_ds = copy_nc(ds, fname, exclude = exclude_vars)

            # Calculate mins and maxes
            for lyr in [l for l in keep_vars if l not in ['x','y','time','projection']]:
                self.ranges[lyr] = [np.min(new_ds.variables[lyr][:]),
                                    np.max(new_ds.variables[lyr][:])]

            # Optional Masking
            if mask != None:
                self.log.info("Masking netcdf using {}...".format(mask))
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

    def copy_data(self, fname, basin, upload_type='modeled'):
        """
        Data for the geoserver has to be in the host location for this. We

        Copies data from users location to geoserver/data/<basin>/

        Args:
            fname: String path to a local file.
            basin: String name of the targeted basin/workspace to put the file
                   in
            upload_type: specifies whether to name a file differently

        Returns:
            final_fname: The remote path to the file we copied
        """
        bname =  os.path.basename(fname)

        final_fname = os.path.join(self.data, basin, bname)
        self.log.info("Copying local data to remote, this may take a couple "
                      "minutes...")

        # Form the SCP command, handle if there is no pem file
        cmd = ["scp"]

        if hasattr(self,"pem"):
            cmd.append("-i")
            cmd.append(self.pem)

        cmd.append(fname)
        cmd.append("{}@{}:{}".format(self.username, self.base_url, final_fname))
        self.log.debug(" ".join(cmd))

        try:
            pass
            #s = sp.check_output(cmd, shell=False, universal_newlines=True)

        except Exception as e:
            self.log.error(e.output)
            raise e

        return final_fname

    def exists(self, basin, store=None, layer=None):
        """
        Checks the geoserver if the object exist already by name. If basin
        store and layer are provided it will check all three and only return
        true if all 3 exist.

        Args:
            basin: String name of the targeted, this script assumes the basin
                   name and workspace are the same.
            store: String name of the data/coverage storage object.
            layer: String name of the layer

        Returns:
            bool: True if all non-None values of the basin,store,layer exists,
                  False otherwise
        """

        store_exists = None
        layer_exists = None

        # We always will check for the basins existance
        ws_exists = False

        # Does the workspace > datastore exist
        if store != None:
            store_exists = False

        # Does the workspace > datastore > layer exist
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

            # Store existance requested
            if store != None:
                # Grab info about this existing workspace
                ws_dict = self.get(w['href'])

                # Grab info on any stores
                cs_dict = self.get(ws_dict['workspace']['coverageStores'])

                # Check if there are any coverage stores
                if cs_dict['coverageStores']:
                    cs_info = cs_dict['coverageStores']

                    # Check for matching name in the coverages
                    for cs in cs_info['coverageStore']:
                        if store == cs['name']:
                            store_exists = True
                            break

            # layer existance requested
            if layer != None and store_exists:
                # Grab info about this existing store
                store_info = self.get(cs['href'])
                coverages = self.get(store_info['coverageStore']['coverages'])

                # Check to see if there any coverages at all
                if coverages['coverages']:
                    for cv in coverages['coverages']['coverage']:
                        if layer == cv['name']:
                            layer_exists = True

        result = [ws_exists, store_exists, layer_exists]
        expected = [r for r in result if r != None]
        truth = [r for r in result if r == True]

        msg = " > ".join([r for r in [basin, store, layer] if r !=None])

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

    def create_coveragestore(self, basin, store, filename, description=None):
        """
        Creates a coverage data store for raster type data on the geoserver.

        Args:
            basin: String name of the targeted basin/workspace
            store: String name of the new coverage data store
            filename: Netcdf to associate with store, must exist locally to
                      geoserver

        """

        bname = os.path.basename(filename)

        # Check to see if the store already exists...
        if self.exists(basin, store=store):

            self.log.warn("Coverage store {} exists!".format(store))

            # Check to see if user wants to delete it and rewrite it
            ans = ask_user("Do you want to overwrite coveragestore {}?"
                           "".format(store), bypass=self.bypass)

            if ans:
                resource = "workspaces/{}/coveragestores/{}.json".format(basin, store)
                self.delete(resource, recurse=True)

            else:
                self.log.info("Unable to continue, exiting...")
                sys.exit()

        # Make the coverage store!
        resource = 'workspaces/{}/coveragestores.json'.format(basin)

        payload = {"coverageStore":{"name":store,
                                    "type":"NetCDF",
                                    "enabled":True,
                                    "_default":False,
                                    "workspace":{"name": basin},
                                    "configure":"all",
                                    "url":"file:basins/{}/{}".format(basin,
                                    bname)}}
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
        native_name = lyr_name#layer.replace('_',' ')

        # Make the names better/ Rename the isnobal stuff
        if native_name in ['snow_density','specific_mass','thickness']:
            name = self.remap[native_name]
        else:
            name = lyr_name

        # Human readable title for geoserver UI
        if name.lower() == 'swe':
            title = ("{} {} {}".format(basin.title(),
                                       self.date,
                                       name.upper())).replace("_"," ")
        else:
            title = ("{} {} {}".format(basin,
                                       self.date,
                                       name)).replace("_"," ").title()

        # Add an associated Date to the layer
        if hasattr(self,'date'):
            name = "{}{}".format(name, self.date.replace('-',''))

        payload = {"coverage":{"name":name,
                               "nativeName":lyr_name,
                               "nativeCoverageName":native_name,
                               "store":{"name": "{}:{}".format(basin, store)},
                               "enabled":True,
                               "title":title,
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
        response = self.make(resource, payload)

        # Assign Colormaps
        self.assign_colormaps(basin, name)

    def assign_colormaps(self, basin, name):
        """
        currently utilizes a hacky version to accomplish our goal. function
        Assigns the colormaps to default and styles available

        Args:
            basin: name of the basin
            name: name of the layer
        """

        resource = ("layers/{}:{}".format(basin, name))


        # All colormaps we want to assign
        colormaps = self.get_keyword_styles(name)

        # Default colormap
        if "dynamic_default" in colormaps:
            colormap = "dynamic_default"
        else:
            colormap = "raster"
        self.log.info("Assigning {} as default colormap.".format(colormap))

        # ##################### Correct way but doesn't work ####################
        #
        # rjson['layer']["styles"] = {"style": [{"name":cm} for cm in colormaps]}
        # rjson["layer"]["defaultStyle"] = {"name": colormap}
        #
        # rjson["layer"]["opaque"] = True
        #
        #r = self.modify(resource, {"layer":{"defaultStyle":{"name":colormap}}})


        ##################### HACK VERSION #####################################
        xml_colormaps = ["\t\t<style><name>{}</name></style>".format(cm) for cm in colormaps]
        xml_colormaps = "\n".join(xml_colormaps)
        xml_entry = "<layer><defaultStyle><name>{}</name></defaultStyle></layer>".format(colormap)

        base_cmd = ["curl","-u","{}:{}".format(self.geoserver_username,
                                          self.geoserver_password),
               "-XPUT", "-H", '"accept:text/xml"', "-H",'"content-type:text/xml"',
               urljoin(self.url,resource+'.xml'),"-s","-d"]

        # Add the default style
        cmd = base_cmd + ['"{}"'.format(xml_entry)]
        self.log.debug("Executing hack:\n{}".format(" ".join(cmd)))
        s = sp.check_output(" ".join(cmd), shell=True, universal_newlines=True)
        self.log.debug(s)

        # Add all the styles available
        xml_entry = "<layer><styles>{}</styles></layer>".format(xml_colormaps)
        cmd = base_cmd + ['"{}"'.format(xml_entry)]
        self.log.debug("Executing hack:\n{}".format(" ".join(cmd)))
        s = sp.check_output(" ".join(cmd), shell=True, universal_newlines=True)
        self.log.debug(s)

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

        self.log.info("{}/{} availables styles are matching".format(len(result), len(avail)))

        # Add in dyanamic_default default if it is there
        if "dynamic_default" in avail:
            result.append('dynamic_default')

        return result

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

            if self.exists(basin, store, name):
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

        # Timing
        self.start = time.time()

        # Reduce the size of netcdfs if possible return the new filename
        filename = self.extract_data(filename, upload_type=upload_type,
                                               espg=espg,
                                               mask=mask)

        # Copy users data up to the remote location
        remote_fname = self.copy_data(filename, basin, upload_type=upload_type)

        # Grab the layer names
        ds = Dataset(filename)

        layers = []
        for name, v in ds.variables.items():
            if name not in ['time','x','y','projection']:
                layers.append(name)

        if len(layers) == 0:
            self.log.error("No variables found in netcdf...exiting.")
            sys.exit()

        # Check for the upload type which determines the filename, and store
        if upload_type == 'topo':
            self.submit_topo(remote_fname, basin, layers=layers)

        elif upload_type == 'modeled':
            self.submit_modeled(remote_fname, basin, layers=layers)

        elif upload_type == 'flight':
            self.log.error("Uploading flights is undeveloped")

        elif upload_type == 'shapefile':
            self.log.error("Uploading shapefiles is undeveloped")

        else:
            raise ValueError("Invalid upload type!")

        # Cleanup
        if self.cleanup:
            self.log.info("Cleaning up files... Removing {}".format(self.tmp))
            rmtree(self.tmp)

        # Timing
        self.log.info("Upload took: {0:0.1f}s".format(time.time() - self.start))
        self.log.info("Complete!\n")

    def submit_topo(self, filename, basin, layers=None):
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

        self.create_coveragestore(basin, store_name, filename,
                                                     description=description)

        self.create_layers_from_netcdf(basin, store_name, filename,
                                                          layers=layers)

    def submit_modeled(self, filename, basin, layers=None):
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

        self.create_coveragestore(basin, store_name, filename,
                                                     description=description)

        # Create layers density, specific mass, thickness
        self.create_layers_from_netcdf(basin, store_name, filename,
                                                          layers=layers)

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

        self.log.info("Download Requested. Attempting to download {} from the {}.".format(fname, basin))

        resource = "resource/basins/{}/{}".format(basin,fname)

        self.grab(resource, fname)

    def submit_styles(self, local_files):
        """
        Uses a post to make the styles available, then uses a put to actually
        move the styles there.
        """
        resource = "styles/"
        existing_styles = self.get(resource)
        existing_styles = [style['name'] for style in existing_styles['styles']['style']]
        self.log.info("Uploading {} styles.".format(len(local_files)))
        self.log.info("{} styles already exist.".format(len(existing_styles)))

        for f in local_files:
            skip = False

            style_name = os.path.basename(f).split('.')[0]
            style_resource = "styles/{}".format(style_name)

            # Check if this is already exists.
            if style_name in existing_styles:
                ans = ask_user("You are about to overwrite the style {}."
                         "\nDo you want to continue?".format(style_name),
                         bypass=self.bypass)
                if ans:
                    self.delete(style_resource)
                else:
                    self.lof.warn("Skipping overwriting {}!".format(style_name))
                    skip = True

            # Upload that bad boy
            if not skip:
                self.log.info("Adding the {} style to the geoserver...".format(style_name))
                payload = {"style":{"name":style_name, "filename":f}}
                resource = "styles/"

                self.make(resource, payload)

                self.move(style_resource, f)

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
        ('{"url":"",\n"remote_username":"",\n"geoserver_username":"",\n'
        '"geoserver_password":"",\n"pem":"",\n"data":""}\n')
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
                             'sanjoaquin','tuolumne'], required=False,
                    help="Basin name to submit to which is also the geoserver"
                         " workspace name")

    p.add_argument('-c','--credentials', dest='credentials',
                    default='./geoserver.json',
                    required=False,
                    help="JSON containing geoserver credentials for logging in")

    p.add_argument('-t','--data_type', dest='data_type',
                    default='modeled',
                    choices=['flight','topo','shapefile','modeled','styles'],
                    required=False,
                    help="Upload/download type dictates how some items are uploaded/downloaded.")

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

    args = p.parse_args()

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
            gs.download(args.basin, args.download, download_type=args.data_type)

        else:
            if args.data_type=="styles":
                if type(args.filenames)!= list:
                    args.filenames = [args.filenames]
                gs.submit_styles(args.filenames)
            else:

                # Upload a file
                gs.upload(args.basin, args.filenames[0], upload_type=args.data_type,
                                                     espg=args.espg,
                                                     mask=args.mask)


if __name__ =='__main__':
    main()
