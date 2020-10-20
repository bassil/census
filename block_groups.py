"""Download blockgroup tiger line files from census.gov and get geoids"""

import glob
import os

from ftplib import FTP
from zipfile import ZipFile

import sqlalchemy

import numpy as np
import pandas as pd
import geopandas as gpd

def get_db_engine(settings):
    """return db engine
    
    parameters
    ----------
    settings: dict
        {'PG_USER': 'sample_user',
         'PG_PASSWORD': 'sample_pw',
         'PG_HOST': 'localhost',
         'PG_PORT': '5432',
         'PG_DATABASE': 'census_block_groups'}

    returns
    -------
    engine: postgresql engine
    """
    
    user = settings['PG_USER']
    passwd = settings['PG_PASSWORD']
    host = settings['PG_HOST']
    port = settings['PG_PORT']
    db = settings['PG_DATABASE']
    
    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    
    engine = create_engine(url, pool_size = 50)
    
    return engine

def download_files(ftp_host, ftp_dir, dl_dir):
    """module that downloads files from ftp_dir of ftp_host into dl_dir

    parameters
    ----------
    ftp_host: string
        ftp host name, e.g., 'ftp2.census.gov'
    ftp_dir: string
        relative path to ftp directory, e.g., 'geo/tiger/TIGER2019/BG'
    dl_dir: string
        absolute path to local directory where files should be written

    returns
    -------
    none
    """
    if not os.path.exists(dl_dir):
        os.mkdir(dl_dir)

    with FTP(ftp_host) as ftp:
        ftp.login()
        ftp.cwd(ftp_dir)
        file_names = ftp.nlst()
        for file_name in file_names:
            file_path = os.path.join(dl_dir, file_name, )

            if not os.path.exists(file_path):
                print("Downloading file: %s..." % file_name)

                with timeit("Downloaded file: %s..." % file_name):
                    with open(file_path, 'wb') as file_obj:
                        ftp.retrbinary('RETR %s' % file_name, file_obj.write)
            else:
                print("File -- %s -- has already been downloaded..." % file_name)


def unzip_files(zipfiles_dir, unzipped_files_dir):
    """module that unzips zip files from zipfiles_dir into subdirectories of unzipped_files_dir

    parameters
    ----------
    zipfiles_dir: string (glob)
        absolute path to directory containing one or more zip files
    unzipped_files_dir: string
        absolute path to directory where unzipped files are stored

    returns
    -------
    none
    """
    file_paths = glob.glob(zipfiles_dir)
    for file_path in file_paths:
        file_path_split = file_path.split('/')
        try:
            file_name, file_extension = file_path_split.pop().split('.')
        except ValueError:
            continue
        if file_extension == "zip":
            # unzip
            if not os.path.exists(unzipped_files_dir):
                os.mkdir(unzipped_files_dir)

            unzipped_file_path = os.path.join(unzipped_files_dir, file_name)
            if not os.path.exists(unzipped_file_path):
                os.mkdir(unzipped_file_path)

            print("Unzipping % s" % file_name + "." + file_extension)
            with timeit("Unzipped % s" % file_name + "." + file_extension):
                with ZipFile(file_path) as zip_obj:
                    zip_obj.extractall(unzipped_file_path)


def combine_shapefiles(shapefiles_dir, combined_shapefiles_path):
    """module that returns a concatenated shapefile gpd dataframe

    parameters
    ----------
    shapefiles_dir: string (glob)
        absolute path to shapefiles directories
    combined_shapefiles_path: string
        absolute path to directory where the concatenated shapefile is stored

    returns
    -------
    concatenated_shapefile: geopandas dataframe
    """
    shapefiles_list = []

    shapefiles_paths = glob.glob(os.path.join(shapefiles_dir, "*.shp",  ))
    for shapefile_path in shapefiles_paths:
        print("Reading shapefile -- %s" % shapefile_path.split("/")[-1])
        shapefile = gpd.read_file(shapefile_path)
        shapefiles_list.append(shapefile)
    # for projection, pass the crs of the first shapefile
    concatenated_shapefile = gpd.GeoDataFrame(pd.concat(shapefiles_list).\
                                                 reset_index().\
                                                 drop(columns=['index']),
                                              crs=shapefiles_list[0].crs)

    if not os.path.exists(combined_shapefiles_path):
        os.mkdir(combined_shapefiles_path)
    print("Concatenating shapefiles...")
    with timeit("Concatenated shapefiles"):
        combined_shapefiles_path = os.path.join(combined_shapefiles_path,
                                                combined_shapefiles_path.split("/")[-1] + ".shp", )

    concatenated_shapefile.to_file(combined_shapefiles_path)

    return concatenated_shapefile


def add_spatial_index_to_db(engine, settings):
    """adds a spatial index on the column containing the geometries"""
    with engine.connect() as connection:
        connection.execute("CREATE INDEX gix ON " + settings['PG_DATABASE'] + " USING GIST (geom)")
        connection.execute("CLUSTER " + settings['PG_DATABASE'] + " USING gix")
        connection.execute("ANALYZE " + settings['PG_DATABASE'])


def add_shapefile_to_db(shapefile_path, settings):
    """adds shapefiles specified in shapefiles_path to db FIXME
    
    Prerequisites:  - postgres installed on host machine
                    - superuser sample_user

    parameters
    ----------
    shapefile_path: string
        /path/to/shapefile, e.g., 
        "/Users/bassil.tabidi/Desktop/work/zipfiles/unzipped/tl_2019_01_bg/tl_2019_01_bg.shp"
    settings: dictionary {string: string}
        e.g., 
        {'PG_USER': 'sample_user',
         'PG_PASSWORD': 'sample_pw',
         'PG_HOST': 'localhost',
         'PG_PORT': '5432',
         'PG_DATABASE': 'census_block_groups'}
    """
    db_name = settings['PG_DATABASE']

    # switch to postgres database temporarily to create db_name database incase
    # db_name database doesn't exist yet
    settings['PG_DATABASE'] = 'postgres'
    engine = get_db_engine(settings)

    with engine.connect() as connection:

        # The connection is open inside a transaction, and postgres does not allow db creation
        # inside a transaction, so we need to end the open transaction
        connection.execution_options(isolation_level="AUTOCOMMIT")
        
        try:
            connection.execute("CREATE DATABASE " + db_name)
        except:
            connection.execute("DROP DATABASE " + db_name)

            connection.execute("CREATE DATABASE " + db_name)
    engine.dispose()

    # get created database
    settings['PG_DATABASE'] = db_name

    engine = get_db_engine(settings)
    with engine.connect() as connection:
        connection.execute("CREATE EXTENSION postgis")

    # create table from shapefile
    print('Creating census_block_groups table:')
    shapefile = gpd.read_file(shapefile_path)
    os.system("shp2pgsql -s 4269 " + shapefile_path + " " + db_name + " | psql -h " + \
              settings['PG_HOST'] + " -d " + db_name + " -U " + settings['PG_USER'])       

    add_spatial_index_to_db(engine, settings)


def create_census_block_group_db(ftp_host, ftp_dir, dl_dir, settings):
    """wrapper for creating a post gis postgresql database of tiger files from ftp"""

    with timeit("Downloading tiger files:"):
        download_files(ftp_host, ftp_dir, dl_dir)

    zipfiles_dir = os.path.join(dl_dir, "*")
    unzipped_files_dir = os.path.join(dl_dir, "unzipped")
    with timeit("Unzipping tiger files:")
        unzip_files(zipfiles_dir, unzipped_files_dir)

    shapefiles_dir = os.path.join(unzipped_files_dir, "*")
    combined_shapefiles_path = os.path.join(unzipped_files_dir, "t1_2019_combined_shapefiles")

    with timeit("Combining unzipped shapefiles:"):
        combine_shapefiles(shapefiles_dir, combined_shapefiles_path)

    shapefiles_path = os.path.join(combined_shapefiles_path, "t1_2019_combined_shapefiles.shp")

    with timeit("Adding shapefiles to postgresql database:"):
        add_shapefile_to_db(shapefiles_path, settings)


def add_block_group_id_to_clusters(clusters, settings):
    """returns clusters with block group id column

    parameters
    ----------
    clusters: pandas dataframe
        includes the columns:
            latitude          float64
            longitude         float64
    settings: dictionary {string: string}
        {'PG_USER': 'sample_user',
         'PG_PASSWORD': 'sample_pw',
         'PG_HOST': 'localhost',
         'PG_PORT': '5432',
         'PG_DATABASE': 'census_block_groups'}
    """
    engine = get_db_engine(settings)
    with engine.connect() as connection:

        def get_block_group_id(lat, lon):
            """returns block group id for a given lat, lon pair

            connects to db w/ census block group id's and geometries,
            and performs a spatial query for the block group id of
            the given lat, lon pair.

            parameters
            ----------
            lat: float64
            lon: float64
            """
            result_proxy = connection.execute("SELECT geoid FROM {} "
                                              "WHERE ST_Contains({}.geom, "
                                              "ST_SetSRID(ST_MakePoint({}, {}), "
                                              "4269))".format(settings['PG_DATABASE'],
                                                              settings['PG_DATABASE'],
                                                              lon,
                                                              lat))
            try:
                return result_proxy.fetchone()['geoid'] 
            except:
                return np.nan
             

        clusters['block_group_id'] = clusters.apply(lambda x: get_block_group_id(x.latitude,
                                                                                 x.longitude),
                                                    axis=1)

    return clusters


# ###########################################################################################
# ######################################### TESTING #########################################
# ###########################################################################################
# Parameters: ###############################################################################
# ftp_host = 'ftp2.census.gov'
# ftp_dir = 'geo/tiger/TIGER2019/BG'
# dl_dir = '/Users/bassil.tabidi/Desktop/work/zipfiles'
# settings = {'PG_USER': 'sample_user',
#             'PG_PASSWORD': 'sample_pw',
#             'PG_HOST': 'localhost',
#             'PG_PORT': '5432',
#             'PG_DATABASE': 'census_block_groups'}
# ###########################################################################################
# Create the db if it doesn't exist: ########################################################
# ###########################################################################################
# create_census_block_group_db(ftp_host, ftp_dir, dl_dir, settings)
# ###########################################################################################
# Load the clusters table into a pandas dataframe: ##########################################
# ###########################################################################################
# tables_dir = '/Users/bassil.tabidi/Desktop/work/data/input_dir/Nashville/tables/output_1'
# clusters_path = os.path.join(tables_dir, 'clusters.csv')
# clusters = pd.read_csv(clusters_path)
#############################################################################################
# ###########################################################################################
# Add the block group ids to clusters: ######################################################
# ###########################################################################################
# clusters = add_block_group_id_to_clusters(clusters, settings)
# ###########################################################################################
