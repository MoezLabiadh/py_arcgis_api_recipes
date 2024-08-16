#-------------------------------------------------------------------------------
# Recipe functions for connecting to ago api and publishing spatial data.
# Attempting to use requests package instead of acgis!!
#-------------------------------------------------------------------------------

import os
import json
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from datetime import datetime
import logging

def df_to_gdf(df, lat_col, lon_col, crs=4326):
    """
    Converts a DataFrame with latitude and longitude columns to a GeoDataFrame
    """
    geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf.set_crs(epsg=crs, inplace=True)  
        
    #convert datetime cols to str: needed to publish to AGO later on
    for column in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[column]):
            gdf[column] = gdf[column].astype(str)
    
    return gdf

def prepare_df_for_ago(df):
    """
    Cleans up df column names and coordinates for AGO.
    """
    df_ago = df.copy()
    
    def clean_name(name):
        # Remove specified special characters
        for char in ['(', ')', '#', '-', ' ', "'"]:
            name = name.replace(char, '')
        return name
    
    # Apply the cleaning function to all column names
    df_ago.columns = [clean_name(col) for col in df_ago.columns]
    
    # Drop rows with NaN values in LatLongSource column
    df_ago = df_ago.dropna(subset=['LatLongSource'])
    
    return df_ago


def get_ago_token(TOKEN_URL, HOST, USERNAME, PASSWORD):
    """
    Returns an access token to AGOL account
    """
    params = {
        'username': USERNAME,
        'password': PASSWORD,
        'referer': HOST,
        'f': 'json'
    }
    
    try:
        # Send request to get token
        response = requests.post(TOKEN_URL, data=params, verify=True) # Enable SSL verification

        # Check response status
        response.raise_for_status()

        logging.info("...successfully obtained AGO access token.")
        
        return response.json().get('token')
    
    except requests.exceptions.RequestException as e:
        logging.error(f"...failed to obtain access token: {e}")
        raise
        
        
def get_ago_folderID(token, username, folder_name):
    """
    Get or create a folder in ArcGIS Online.
    """
    folders_url = f"https://www.arcgis.com/sharing/rest/content/users/{username}"
    params = {
        'f': 'json',
        'token': token,
    }

    try:
        # Check if the folder exists
        response = requests.post(folders_url, data=params, verify=True)
        response.raise_for_status()
        folders = response.json().get('folders', [])
        
        for folder in folders:
            if folder['title'] == folder_name:
                logging.info(f"...folder '{folder_name}' already exists.")
                return folder['id']
        
        # Create the folder if it does not exist
        create_folder_url = f"{folders_url}/createFolder"
        create_params = {
            'f': 'json',
            'title': folder_name,
            'token': token
        }
        
        create_response = requests.post(create_folder_url, data=create_params, verify=True)
        create_response.raise_for_status()
        
        logging.info(f"...folder '{folder_name}' created successfully.")
        return create_response.json().get('folder').get('id')
    
    except requests.exceptions.RequestException as e:
        logging.error(f"...failed to create or get folder: {e}")
        raise
            

def create_feature_service(token, username, folder_id, service_name):
    """
    Creates a Feature Service in ArcGIS online
    """
    params = {
        'f': 'json',
        'token': token,
        'createParameters': json.dumps({
            'name': service_name,
            'serviceDescription': '',
            'hasStaticData': False,
            'maxRecordCount': 1000,
            'supportedQueryFormats': 'JSON',
            'capabilities': 'Create,Delete,Query,Update,Editing',
            'initialExtent': {
                'xmin': -180,
                'ymin': -90,
                'xmax': 180,
                'ymax': 90,
                'spatialReference': {'wkid': 4326}
            },
            'allowGeometryUpdates': True,
            'units': 'esriDecimalDegrees',
            'xssPreventionInfo': {'xssPreventionEnabled': True, 'xssPreventionRule': 'InputOnly', 'xssInputRule': 'rejectInvalid'}
        }),
        'tags': 'feature service',
        'title': service_name
    }
    CREATE_SERVICE_URL = f'https://www.arcgis.com/sharing/rest/content/users/{username}/{folder_id}/createService'
    response = requests.post(CREATE_SERVICE_URL, data=params)
    response_json = response.json()
    if 'serviceItemId' in response_json and 'encodedServiceURL' in response_json:
        service_id = response_json['serviceItemId']
        service_url = response_json['encodedServiceURL']
        admin_url = service_url.replace('/rest/', '/rest/admin/')
        logging.info(f"...feature service created successfully with ID: {service_id}")
        return service_id, admin_url
    else:
        raise Exception(f"Error creating feature service: {response_json}")


def add_layer_to_service(token, admin_url, df, latcol, longcol):
    """
    Adds a Point Layer to the Feature Service
    """
    add_to_definition_url = f"{admin_url}/addToDefinition"

    fields = [{
        "name": "ObjectID",
        "type": "esriFieldTypeOID",
        "alias": "ObjectID",
        "sqlType": "sqlTypeOther",
        "nullable": False,
        "editable": False,
        "domain": None,
        "defaultValue": None
    }]
    
    for col in df.columns:
        fields.append({
            "name": col,
            "type": "esriFieldTypeString",  # Assuming all fields are string
            "alias": col,
            "sqlType": "sqlTypeOther",
            "nullable": True,
            "editable": True,
            "domain": None,
            "defaultValue": None
        })

    # Calculate extent from the lat/long columns
    xmin = df[longcol].min()
    ymin = df[latcol].min()
    xmax = df[longcol].max()
    ymax = df[latcol].max()

    layer_definition = {
        "layers": [
            {
                "name": "Points",
                "type": "Feature Layer",
                "geometryType": "esriGeometryPoint",
                "fields": fields,
                "extent": {
                    "xmin": xmin,
                    "ymin": ymin,
                    "xmax": xmax,
                    "ymax": ymax,
                    "spatialReference": {"wkid": 4326}
                }
            }
        ]
    }

    params = {
        'f': 'json',
        'token': token,
        'addToDefinition': json.dumps(layer_definition)
    }
    response = requests.post(add_to_definition_url, data=params)

    response_json = response.json()
    if 'success' in response_json and response_json['success']:
        logging.info("...layer added to the feature service successfully")
        return True
    else:
        raise Exception(f"Error adding layer to feature service: {response_json}")
        
        
def add_features(token, service_url, df, latcol, longcol):
    """
    Adds data (features) to the Feature Service Layer
    """
    add_features_url = f"{service_url}/0/addFeatures"
    features = []
    for index, row in df.iterrows():
        try:
            attributes = {}
            for col in df.columns:
                value = row[col]
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif pd.isna(value):
                    value = None
                attributes[col] = value

            feature = {
                'geometry': {
                    'x': float(row[longcol]),
                    'y': float(row[latcol]),
                    'spatialReference': {'wkid': 4326}
                },
                'attributes': attributes
            }
            features.append(feature)
        except Exception as e:
            logging.error(f"Error processing row {index}: {e}")

    if not features:
        logging.error("No valid features to add")
        return []

    params = {
        'f': 'json',
        'token': token,
        'features': json.dumps(features)
    }
    
    try:
        response = requests.post(add_features_url, data=params)
        response.raise_for_status()
        response_json = response.json()
        
        if 'addResults' in response_json:
            successful_adds = sum(1 for result in response_json['addResults'] if result.get('success', False))
            logging.info(f"...{successful_adds} out of {len(features)} features added successfully")
            return response_json['addResults']
        else:
            logging.error(f"Error adding features: {response_json}")
            return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return []
