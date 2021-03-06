#  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
import botocore
import boto3
import pandas as pd
import io
import os
import urllib.parse
import random, time
import json
import datetime 

###  This function takes a raw data shard from the "raw" bucket, 
###  uses AWS Locations to GeoCode/ReverseGeoCode based on the columns in the datasets, 
###  and puts the processed shard into a "processed" bucket.
region = os.environ['AWS_REGION']

s3_client = boto3.client('s3',region_name=region)
location = boto3.client('location',region_name=region)
destination_bucket = os.environ.get('PROCESSED_SHARDS_BUCKET')
location_index = os.environ.get('LOCATION_INDEX')
stepfunctions_client = boto3.client('stepfunctions',region_name=region)
ddb_table = os.environ.get("DDB_TABLE_NAME")
ddb_client = boto3.client("dynamodb")
dynamodb = boto3.resource('dynamodb')




def get_location_for_position(IndexName, Position, MAX_RETRIES = 5):
    retry = True
    retries = 1
    while (retry and (retries < MAX_RETRIES)):
        try:
            response = location.search_place_index_for_position(
                IndexName=location_index,
                Position=Position)
            retry = False
            return(response)
        except botocore.exceptions.ClientError as error:
            # wait for (2^retries * 100) milliseconds
            time.sleep(2**retries * 100/1000)
            if error.response['Error']['Code'] == 'ThrottlingException':
                print('{}: API call limit exceeded; backing off and retrying...{}: retries: {}'.format(Position,error.response['Error']['Code'],retries))
                retry = True
            elif error.response['Error']['Code'] == 'InternalServerException':
                print('{}: Internal Server Exception; backing off and retrying...{}: retries: {}'.format(Position,error.response['Error']['Code'],retries))
                retry = True
            elif error.response['Error']['Code'] == 'ValidationException':
                print('{}: Exiting...{}'.format(Position,error.response['Error']['Code']))
                retry = False
            elif error.response['Error']['Code'] == 'AccessDeniedException':
                print('{}: Exiting...{}'.format(Position, error.response['Error']['Code']))    
                retry = False
            elif error.response['Error']['Code'] == 'ResourceNotFoundException':
                print('{}: Exiting...{}'.format(Position,error.response['Error']['Code']))   
                retry = False
            elif error.response['Error']['Code'] == 'TooManyRequestsException':
                print('{}: API call limit exceeded; backing off and retrying...{}: retries: {}'.format(Position,error.response['Error']['Code'],retries))  
                retry = True
            else:    
                print ("{}: Un-Identified Exception: {} || {}".format(Position, error, error.response['Error']['Code']))
                retry = False
            retries = retries + 1
    print("{}: Giving up... Too many retries..".format(Position))
    return("Error")

def write_location_to_cache (table_name, location_to_cache, MAX_RETRIES = 10):
    retry = True
    retries = 1
    week = datetime.datetime.today() + datetime.timedelta(days=1)
    expiryDateTime = int(time.mktime(week.timetuple()))
    try:
        response = ddb_client.put_item(
        TableName=table_name,
        Item={
            "id": {
                "S": location_to_cache["PrimaryKey"]
              },
              "Geometry": {
                "S": json.dumps(location_to_cache["Geometry"])
              },
              "Country": {
                "S": location_to_cache["Country"]
              },
              "Zipcode": {
                "S": location_to_cache["Zipcode"]
              },
              "Latitude": {
                "N": location_to_cache["Latitude"]
              },
              "Longitude": {
                "N": location_to_cache["Longitude"]
              },
              "Label": {
                "S": location_to_cache["Label"]
              },
              "Municipality": {
                "S": location_to_cache["Municipality"]
              },
              "Region": {
                "S": location_to_cache["Region"]
              },
              "SubRegion": {
                "S": location_to_cache["SubRegion"]
              },
              "ttl": {
                "N": str(expiryDateTime)
              }
                })
        return(response)
    except Exception as e:
        print({"error":"cannot write to cache", "exception":str(e)})
        return({"error":"cannot write to cache", "exception":str(e)}) 

def get_location_from_cache (table_name, primary_key, MAX_RETRIES = 10):
    retry = True
    retries = 1
    
    try:
        response = ddb_client.get_item(TableName=table_name, Key={"id": { "S": primary_key}})
        cached_location = {}
        if 'Item' in response:
            for key, value in response['Item'].items():
                if key == 'Geometry':
                    cached_location[key] = json.loads(value['S'])
                if key == 'Country':
                    cached_location[key] = value['S']
                if key == 'Zipcode':
                    cached_location[key] = value['S']
                if key == 'Latitude':
                    cached_location[key] = value['N']
                if key == 'Longitude':
                    cached_location[key] = value['N']
                if key == 'Label':
                    cached_location[key] = value['S']
                if key == 'Municipality':
                    cached_location[key] = value['S']
                    
                if key == 'Region':
                    cached_location[key] = value['S']
                    
                if key == 'SubRegion':
                    cached_location[key] = value['S']

            return(cached_location)
    
        else:

            return({"error":"not found"})
        
        
    except Exception as e:
        print({"error":"cannot read from cache", "exception":str(e)})
        return({"error":"cannot read from cache", "exception":str(e)})
    

def get_location_for_text (IndexName, Text, MAX_RETRIES = 10):
    retry = True
    retries = 1
    while (retry and (retries < MAX_RETRIES)):
        try:
            response = location.search_place_index_for_text(
                IndexName=location_index,
                Text=Text)
            retry = False
            return(response)
        except botocore.exceptions.ClientError as error:
            # wait for (2^retries * 100) milliseconds
            time.sleep(2**retries * 100/1000)
            # status = Get the result of the asynchronous operation.
            if error.response['Error']['Code'] == 'ThrottlingException':
                print('{}: API call limit exceeded; backing off and retrying...{}: retries: {}'.format(Text,error.response['Error']['Code'],retries))
                retry = True
            elif error.response['Error']['Code'] == 'InternalServerException':
                print('{}: Internal Server Exception; backing off and retrying...{}: retries: {}'.format(Text,error.response['Error']['Code'],retries))
                retry = True
            elif error.response['Error']['Code'] == 'ValidationException':
                print('{}: Exiting...{}'.format(Text,error.response['Error']['Code']))
                retry = False
            elif error.response['Error']['Code'] == 'AccessDeniedException':
                print('{}: Exiting...{}'.format(Text,error.response['Error']['Code']))    
                retry = False
            elif error.response['Error']['Code'] == 'ResourceNotFoundException':
                print('{}: Exiting...{}'.format(Text,error.response['Error']['Code']))   
                retry = False
            elif error.response['Error']['Code'] == 'TooManyRequestsException':
                print('{}: API call limit exceeded; backing off and retrying...{}: retries: {}'.format(Text,error.response['Error']['Code'],retries))  
                retry = True
            else:    
                print ("{}: Un-Identified Exception: {} || {}".format(Text,error, error.response['Error']['Code']))
                retry = False
            retries = retries + 1
    print("{}: Giving up... Too many retries..".format(Text))
    return("Error")
        
        
def lambda_handler(event, context):
    
    ################################################################
    #     Get Pre-Processed Shard from S3 via a triggered GET      #
    ################################################################
    bucket_name = event["Payload"]["bucket"]
    s3_file_key = event["Payload"]["shard"]

    # s3_file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') 
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        data = pd.read_csv(response.get("Body")).dropna(thresh=2)
        data = data.rename(columns=str.title)
        columns = data.columns
        Countries = []
        Points = []
        Latitude = []
        Longitude = []
        Labels = []
        Regions = []
        SubRegions = []
        Municipalities = []
        Zipcodes = []
        Latitudes = []
        Longitudes = []
        location_to_cache = {}
        ###########################
        #     ReverseGeocoder     #
        ###########################
        
        if "Latitude" and "Longitude" in columns:
            for index, row in data.iterrows():
                try:
                    response_from_cache = get_location_from_cache (ddb_table, str(row.Longitude) +","+ str(row.Latitude))
                    if "error" in response_from_cache:
                        try:
                            print("Making API call to Places API")
                            response = get_location_for_position(location_index, [row.Longitude, row.Latitude])
                            json_response = response["Results"][0]["Place"]
                            print(json_response)
                        except Exception as e:
                            print("API Response Error: " + str(e))
                            print("API Response Error")
                    else:
                        print("Found Location in Cache")
                        
                        json_response = response_from_cache
                        print(json_response)
                        json_response['Geometry']['Point'] = json.loads(json_response['Geometry']['Point'])
                except Exception as e:
                    print("Exception reading from Cache: " + str(e))
                try:
                    Country = (json_response["Country"])
                    Countries.append(Country)
                except Exception as e:
                    Country = "0"
                    Countries.append(0)
                try:
                    Point = (json_response["Geometry"]["Point"])
                    Points.append(Point)
                except Exception as e:
                    Point = "0"
                    Points.append(0)
                try:
                    Longitude = (Point[0])
                    print("Longitude: {}".format(Longitude))
                    Longitudes.append(Longitude)
                except Exception as e:
                    Longitude = "0"
                    Longitudes.append(0)
                    print("Error: Lon unavailable for given input in row", (len(Points)) + 1)
                try:
                    Latitude = (Point[1])
                    print("Latitude: {}".format(Latitude))
                    Latitudes.append(Latitude)
                except Exception as e:
                    Latitude = "0"
                    Latitudes.append(0)
                    print("Error: Lat unavailable for given input in row", (len(Points)) + 1)
                try:
                    Label = (json_response["Label"])
                    Labels.append(Label)
                except Exception as e:
                    Label = "0"
                    Labels.append(0)
                    print("Error: Address unavailable for given input in row", (len(Points)) + 1)
                try:
                    Zipcode = (json_response["PostalCode"])
                    Zipcodes.append(Zipcode)
                except Exception as e:
                    Zipcode = "0"
                    Zipcodes.append(0)
                try:
                    if "Municipality" in (json_response):
                         Municipality = (json_response["Municipality"])
                         Municipalities.append(Municipality)
                    else:
                         Municipality = "0"
                         Municipalities.append(0)
                except Exception as e:
                    Municipality = "0"
                    Municipalities.append(0)
                try:
                    Region = (json_response["Region"])
                    Regions.append(Region)
                except Exception as e:
                    Region = "0"
                    Regions.append(0)
                    print("Error: Region unavailable for given input in row", (len(Points)) + 1)
                try:
                    SubRegion = (json_response["SubRegion"])
                    SubRegions.append(SubRegion)
                except Exception as e:
                    SubRegion = "0"
                    SubRegions.append(0)
                    print("Error: SubRegion unavailable for given input in row", (len(Points)) + 1)
                
                location_to_cache["Geometry"]={}
                location_to_cache["Geometry"]["Point"] = str(Point)
                location_to_cache["Country"] = Country
                location_to_cache["Zipcode"] = Zipcode
                location_to_cache["Latitude"] = str(Latitude)
                location_to_cache["Longitude"] = str(Longitude)
                location_to_cache["Label"] = Label
                location_to_cache["Municipality"] = Municipality
                location_to_cache["Region"] = Region
                location_to_cache["SubRegion"] = SubRegion
                location_to_cache["PrimaryKey"] = str(row.Longitude) +","+ str(row.Latitude)
                if "error" in response_from_cache:
                    print("Writing to Cache: {}".format(location_to_cache["PrimaryKey"]))
                    write_location_to_cache (ddb_table, location_to_cache)
            print ("length of Points: {}".format(len(Points)))
            print ("length of Countries: {}".format(len(Countries)))
            print ("length of Latitude: {}".format(len(Latitudes)))
            print ("length of Longitude: {}".format(len(Longitudes)))
            print ("length of Labels: {}".format(len(Labels)))
            print ("length of Municipalities: {}".format(len(Municipalities)))
            print ("length of Regions: {}".format(len(Regions)))
            print ("length of SubRegions: {}".format(len(SubRegions)))
            data["Points"] = Points
            data["Country"] = Countries
            data["Latitude"] = Latitudes
            data["Longitude"] = Longitudes
            data["Label"] = Labels
            data["Municipality"] = Municipalities
            data["Region"] = Regions
            data["SubRegion"] = SubRegions
            data["Zipcode"] = Zipcodes
            
        #########################################################
        #     Geocoder  (for different possible column labels)  #
        #########################################################
        

        elif "Address" in columns:
            for index, row in data.iterrows():
                
                try:
                    response_from_cache = get_location_from_cache (ddb_table, str(row.Address) + row.City + "," + row.State)
                    if "error" in response_from_cache:
                        try:
                            print("Making API call to Places API")
                            response = get_location_for_text(location_index, str(row.Address) + row.City + "," + row.State)
                            json_response = response["Results"][0]["Place"]
                            print(json_response)
                        except Exception as e:
                            print("API Response Error: " + str(e))
                            print("API Response Error")
                    else:
                        print("Found Location in Cache")
                        json_response = response_from_cache
                        json_response['Geometry']['Point'] = json.loads(json_response['Geometry']['Point'])
                        print(json_response)
                except Exception as e:
                    print("Exception reading from Cache: " + str(e))
                try:
                    Country = (json_response["Country"])
                    Countries.append(Country)
                except Exception as e:
                    Country = "0"
                    Countries.append(0)
                try:
                    Point = (json_response["Geometry"]["Point"])
                    Points.append(Point)
                except Exception as e:
                    Point = "0"
                    Points.append(0)
                try:
                    Longitude = (Point[0])
                    print("Longitude: {}".format(Longitude))
                    Longitudes.append(Longitude)
                except Exception as e:
                    Longitude = "0"
                    Longitudes.append(0)
                    print("Error: Lon unavailable for given input in row", (len(Points)) + 1)
                try:
                    Latitude = (Point[1])
                    print("Latitude: {}".format(Latitude))
                    Latitudes.append(Latitude)
                except Exception as e:
                    Latitude = "0"
                    Latitudes.append(0)
                    print("Error: Lat unavailable for given input in row", (len(Points)) + 1)
                try:
                    Label = (json_response["Label"])
                    Labels.append(Label)
                except Exception as e:
                    Label = "0"
                    Labels.append(0)
                    print("Error: Address unavailable for given input in row", (len(Points)) + 1)
                try:
                    Zipcode = (json_response["PostalCode"])
                    Zipcodes.append(Zipcode)
                except Exception as e:
                    Zipcode = "0"
                    Zipcodes.append(0)
                try:
                    if "Municipality" in (json_response):
                         Municipality = (json_response["Municipality"])
                         Municipalities.append(Municipality)
                    else:
                         Municipality = "0"
                         Municipalities.append(0)
                except Exception as e:
                    Municipality = "0"
                    Municipalities.append(0)
                try:
                    Region = (json_response["Region"])
                    Regions.append(Region)
                except Exception as e:
                    Region = "0"
                    Regions.append(0)
                    print("Error: Region unavailable for given input in row", (len(Points)) + 1)
                try:
                    SubRegion = (json_response["SubRegion"])
                    SubRegions.append(SubRegion)
                except Exception as e:
                    SubRegion = "0"
                    SubRegions.append(0)
                    print("Error: SubRegion unavailable for given input in row", (len(Points)) + 1)
                location_to_cache["Geometry"]={}
                location_to_cache["Geometry"]["Point"] = str(Point)
                location_to_cache["Country"] = Country
                location_to_cache["Zipcode"] = Zipcode
                location_to_cache["Latitude"] = str(Latitude)
                location_to_cache["Longitude"] = str(Longitude)
                location_to_cache["Label"] = Label
                location_to_cache["Municipality"] = Municipality
                location_to_cache["Region"] = Region
                location_to_cache["SubRegion"] = SubRegion
                location_to_cache["PrimaryKey"] = str(row.Address) + str(row.City) + "," + str(row.State)
                if "error" in response_from_cache:
                    write_location_to_cache (ddb_table, location_to_cache)
            print ("length of Points: {}".format(len(Points)))
            print ("length of Countries: {}".format(len(Countries)))
            print ("length of Latitude: {}".format(len(Latitudes)))
            print ("length of Longitude: {}".format(len(Longitudes)))
            print ("length of Labels: {}".format(len(Labels)))
            print ("length of Municipalities: {}".format(len(Municipalities)))
            print ("length of Regions: {}".format(len(Regions)))
            print ("length of SubRegions: {}".format(len(SubRegions)))
            data["Points"] = Points
            data["Country"] = Countries
            data["Latitude"] = Latitudes
            data["Longitude"] = Longitudes
            data["Label"] = Labels
            data["Municipality"] = Municipalities
            data["Region"] = Regions
            data["SubRegion"] = SubRegions
            data["Zipcode"] = Zipcodes
        
        ################################################## 
        #     Write processed shard to S3 via a PUT      #
        ##################################################
        response_lambda={}
        response_lambda['Payload']={}
        with io.StringIO() as csv_buffer:
            data.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(
                Bucket=destination_bucket, Key=s3_file_key, Body=csv_buffer.getvalue()
                )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
                response_lambda['Payload']={"shard": s3_file_key}
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
                response_lambda['Payload']={"status": status}
        
    
    return(response_lambda)
