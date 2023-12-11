
import json
import os
import boto3
import time
import gzip
import shutil
import random
import botocore
from botocore.exceptions import ClientError
from datetime import datetime
import numpy as np
import wave
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import urllib.parse
import logging
import math
# import matplotlib.pyplot as plt

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


###############################

def wait_until_object_exists(bucket_name, key_name, delay, max_attempts):

    session = boto3.session.Session()
    s3_client = session.client('s3')

    logger.info(f'Waiting for file: {bucket_name}/{key_name}')
    logger.info(f'Delay between tries: {delay}   |   Max attempts: {max_attempts}')

    try:
        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=bucket_name, Key = key_name,
            WaiterConfig={
                'Delay': delay, 'MaxAttempts': max_attempts})
        logger.info('Object exists: ' + bucket_name +'/'+key_name)
        return True

    except botocore.exceptions.WaiterError as e:
        # The object does not exist.
        logger.error("Something went wrong. Maybe the file doesn't exist yet? Perhaps lambda has insufficient permissions to access the file?")
        logger.error("Error messsage: " + e.__str__())

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            logger.error(bucket_name + '/' + key_name + " does not exist.")
        else:
            # Something else has gone wrong.
            raise Exception( "Unexpected error in wait_until_object_exists: " + e.__str__())

    except Exception as e:
            # Something else has gone wrong.
            raise Exception( "Unexpected error in wait_until_object_exists: " + e.__str__())

###############################

def get_secret(secret_name):

    #secret_name = "valmetdna_snowflake"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logger.error("Error messsage: " + e.__str__())
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("An error occurred on the server side.")
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("You provided an invalid value for a parameter.")
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("You provided a parameter value that is not valid for the current state of the resource.")
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error("We can't find the resource that you asked for.")
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secret

###############################

def ExecuteAthenaQueryWithRetry(athena, catalog, database, athena_temp, sql, retries = 10):

    query = sql

    logger.info("Attempting Athena query '{}'".format(sql))

    tries = 1
    sleeptime = 30
    status = ''

    do_retry = True

    while do_retry:

        logger.info("query retry {}/{}".format(tries, retries))

        if tries > retries:
            logger.info("Query retry attempt {}/{}. No success. Return error.".format(tries, retries))
            status = 'Throttling retry limit reached'
            do_retry = False

        else:

            try:

                # Run query to Athena
                response = athena.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={
                        'Catalog' : catalog,
                        'Database': database
                    },
                    ResultConfiguration={
                        'OutputLocation': 's3://' + athena_temp
                    })

                # Poll query result
                execution_id = response['QueryExecutionId']

                while True:
                    stats = athena.get_query_execution(QueryExecutionId=execution_id)
                    status = stats['QueryExecution']['Status']['State']
                    if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                        # do_retry = False
                        #try:
                        #    logger.info("Error message: " + stats['QueryExecution']['Status']['AthenaError']['ErrorMessage'])
                        #except KeyError as exception:
                        #    logger.info("AthenaError not found")
                        break
                    time.sleep(0.2)  # 200ms

            except ClientError as exception:
                if exception.response['Error']['Code'] == 'ThrottlingException':
                    if tries <= retries:
                        randomsleep = sleeptime + random.randint(1, 30) + random.randint(1, 30)
                        logger.info(
                            "Query ThrottlingException: retry attempt {}/{}. Sleep {} seconds".format(tries, retries, randomsleep))
                        time.sleep(randomsleep)
                        tries = tries + 1
                        logger.info("next try...")
                    else:
                        logger.info(
                            "Query ThrottlingException: retry attempt {}/{}. No success. Return error.".format(tries, retries))
                        raise
                elif exception.response['Error']['Code'] == 'TooManyRequestsException':
                    if tries <= retries:
                        randomsleep = sleeptime + random.randint(3, 20) + random.randint(1, 30) + random.randint(1, 30)
                        logger.info(
                            "Query TooManyRequestsException: retry attempt {}/{}. Sleep {} seconds".format(tries, retries, randomsleep))
                        time.sleep(randomsleep)
                        tries = tries + 1
                        logger.info("next try...")
                    else:
                        logger.info(
                            "Query TooManyRequestsException: retry attempt {}/{}. No success. Return error.".format(tries, retries))
                        raise
                else:
                    raise

        result = False

        if status == 'SUCCEEDED':
            logger.info("Query SUCCEEDED: {}".format(execution_id))
            result = True
            return result
        else:
            logger.error("Query Failed: {}".format(status))
            result = False
            tries = tries + 1
        if tries > retries:
            logger.info(stats)
            return result

def lambda_handler(event, context):

    s3 = boto3.resource('s3')

    # Retrieve File Information from event
    source_bucket = event['Records'][0]['s3']['bucket']['name'] # in account "latausalue-tuotanto"
    s3_file_name = event['Records'][0]['s3']['object']['key']
    s3_file_name = urllib.parse.unquote(s3_file_name)
    # If file extension isn't wav.gz, stop lambda
    if not s3_file_name.endswith(".wav.gz"):
        logger.info("Not a .wav.gz -file, nothing to do.")
        return

    # Get environment variables
    dest_bucket = os.getenv('DEST_BUCKET', "rata-vaihdedata-dw-dev") # in account "analytiikkapilvi-kehitys"
    dest_raw_bucket = os.getenv('DEST_RAW_BUCKET', "rata-vaihdedata-raw-dev") # in account "analytiikkapilvi-kehitys", stores the original files
    wait_time_if_exist = os.getenv('DELAY_FOR_JSON', 2)
    retries_if_exist = os.getenv('RETRY_FOR_JSON', 5)
    region = os.environ.get('AWS_REGION', "eu-west-1")
    debug_bucket = os.environ.get('DEBUG_BUCKET', "rata-vaihdedata-vrfleetcare-failedinput-dev")
    debug_prefix = os.environ.get('DEBUG_PREFIX', "")
    athena_database = os.environ.get('ATHENA_DATABASE', "vaihdedata-test")
    too_long_prefix = os.environ.get('TOO_LONG_PREFIX', "too-long/")
    limit_sample = bool(os.environ.get('LIMIT_SAMPLE', "False"))
    sample_max_length = float(os.environ.get('SAMPLE_MAX_LENGTH', 15))

    # athena_temp = os.environ.get('ATHENA_TEMP_BUCKET', "analytiikka-prod-athena-temp")

    # Get filename and and path 
    if len(s3_file_name.rsplit('/',1))>1:
        file_path = s3_file_name.rsplit('/',1)[0]  
        base_name = s3_file_name.rsplit('/',1)[1] 
    else:
         file_path =''
         base_name = s3_file_name

    # Get filename without .wav.gz extension
    base_name = base_name.rsplit('.',2)[0]

    # key argument comes in as urlencoded.. decode for finding the path 
    # base_name = urllib.parse.unquote(base_name)
   
    # Trigger from wav.gz and the look for matching json

    # Expected Json file
    expected_json = file_path + "/" + base_name + ".json"
    logger.info('Expecting json: ' + expected_json)

    # Checking if the json file exist
    if wait_until_object_exists(source_bucket, expected_json, wait_time_if_exist, retries_if_exist):
        logger.info(expected_json + " found from: " + source_bucket + ", proceeding with lambda...")
    else:
        logger.error('File "' + expected_json + '" not found from: ' + source_bucket + '. Stopping lambda...')
        return

    # wav.gz and json come as a pair
    signal_key = s3_file_name    # "VRFC/HLT/V0234/A/3/14:18:44.827000.wav.gz"
    metadata_key = expected_json # "VRFC/HLT/V0234/A/3/14:18:44.827000.json"
    # Event id is timestamp of event. Date comes from json. This is parsed from keys 
    event_id = base_name         # "14:18:44.827000"

    # # key signal_key comes in as urlencoded.. decode for finding the path 
    # signal_key = urllib.parse.unquote(signal_key)
    # event_id = urllib.parse.unquote(event_id)
    # metadata_key = urllib.parse.unquote(metadata_key)

    temp_filename = event_id.replace(":","_")
    temp_filename = temp_filename.replace(".","_")
    local_temppath = "/tmp/"

    # Get credentials from AWS Parameter Store
    ## region = "eu-west-1"
    #source_session = boto3.Session(profile_name = "latausalue")
    source_session = boto3.Session()

    # Change print()s to log writes that can be turned off 
    logger.info("Source credentials ok")

    #dest_session = boto3.Session(profile_name = "analytiikka_prod")
    dest_session = boto3.Session(
        #aws_access_key_id=ACCESS_KEY,
        #aws_secret_access_key=SECRET_KEY,
        #aws_session_token=SESSION_TOKEN
        )

    logger.info("Destination credentials ok")

    # S3 Client for reading from source account    
    source_s3_client = source_session.client('s3', region_name=region)
    source_s3_resource = source_session.resource('s3', region_name=region)

    logger.info("Seeking " + signal_key + " from " + source_bucket) 
        
    source_s3_client.download_file(source_bucket, signal_key, local_temppath + temp_filename + ".wav.gz")

    logger.info("Found " + signal_key + " from " + source_bucket) 

    logger.info("Looking for metadata in " + metadata_key + " from " + source_bucket) 

    metadata_object = source_s3_resource.Object(source_bucket, metadata_key)
    metadata_content = metadata_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(metadata_content)

    logger.info("Parsing metadata...")

    country_code = json_content['device_data']['location']['country_code']

    # this is location/location in recent data
    try:
        location = json_content['device_data']['location']['city']
    except KeyError as e:
        try:
            location = json_content['device_data']['location']['location']
        except KeyError as e:
            raise Exception("Location error, there is something wrong with the JSON: " + e.__str__())
            
    
    try:
        vayla_code = json_content['device_data']['location']['location_code']
    except KeyError as e:    
        try:
            vayla_code = json_content['device_data']['location']['localtion_code']
        except KeyError as e:
            try:
                vayla_code = json_content['device_data']['location']['vayla_code']
            except KeyError as e:
                raise Exception("Vayla code error, there is something wrong with the JSON: " + e.__str__())

   

    logger.info("- location: {}/{}/{}".format(country_code, location, vayla_code))

    try:
        switch_id = json_content['device_data']['location']['measured_asset']['id']
    except KeyError as e:
        try:
            switch_id = json_content['device_data']['location']['switch']['id']
        except KeyError as e:
            raise Exception("Switch ID error, there is something wrong with the JSON: " + e.__str__())
    
    try:
        switch_turningdevice = json_content['device_data']['location']['measured_asset']['measured_subasset']
    except KeyError as e:
        try:
            switch_turningdevice = json_content['device_data']['location']['switch']['turning_device']
        except KeyError as e:
            raise Exception("Switch turningdevice error, there is something wrong with the JSON: " + e.__str__())

    try:
        switch_wire_num = json_content['device_data']['location']['measured_asset']['measured_component']
    except KeyError as e:
        try:
            switch_wire_num = json_content['device_data']['location']['switch']['wire_num']
        except KeyError as e:
            raise Exception("Switch wire number error, there is something wrong with the JSON: " + e.__str__())


    # switch_id = json_content['device_data']['location']['switch']['id']
    # switch_turningdevice = json_content['device_data']['location']['switch']['turning_device']
    # switch_wire_num = json_content['device_data']['location']['switch']['wire_num']

    # Only switch and turning device is used in the partition key. Wire num is at the metadata.
    switch = "{}-{}".format(switch_id, switch_turningdevice)

    logger.info("- switch id: " + switch)

    event_timestring = json_content['precalculated_data']['datetime']

    # Timestamp comes in format  "2022-10-28T14:18:44.827000"
    event_starttime = datetime.strptime(event_timestring, '%Y-%m-%dT%H:%M:%S.%f')
    
    logger.info("- event date: " + event_starttime.strftime("%d.%m.%Y"))
    logger.info("- event start time: " + event_starttime.strftime("%H:%M:%S.%f"))

    # this is different in test data

    #device_channel = json_content['device_data']['channel']
    #device_channel = json_content['device_data']['configuration']['channel_configuration']
    device_channel = 1

    # test data fix 

    #device_code = json_content['device_data']['device_code']
    #device_code = json_content['device_data']['device_id']

    try:
        device_code = json_content['device_data']['device_id']
    except KeyError as e:
        try:
            device_code = json_content['device_data']['device_code']
        except KeyError as e:
            raise Exception("Device code/id error, there is something wrong with the JSON: " + e.__str__())


    # device_configuration = ""
    try:
        measurement_range =  json_content['device_data']['configuration']['measurement_range_value']
    except TypeError as e:
        if e.__str__() == "'NoneType' object is not subscriptable":
            logger.info("measurement_range not found.")
        logger.debug("Debug: " + e.__str__())
        logger.info("measurement_range is left empty.")
        measurement_range = ""; 

    logger.info("- device: {}/{}/{}".format(device_code, device_channel, measurement_range))

    precalculated_bitrate = json_content['precalculated_data']['bit_rate']
    precalculated_duration = float(json_content['precalculated_data']['duration'])
    precalculated_energy = float(json_content['precalculated_data']['energy'])
    precalculated_max_current = float(json_content['precalculated_data']['max_current'])
    precalculated_median_current = float(json_content['precalculated_data']['median_current'])
    precalculated_min_current = float(json_content['precalculated_data']['min_current'])
    precalculated_sample_rate = json_content['precalculated_data']['sample_rate']

    logger.info("- precalculated: {}/{}/{}/{}/{}/{}/{}".format(precalculated_bitrate, precalculated_duration, precalculated_energy, precalculated_max_current, precalculated_median_current, precalculated_min_current, precalculated_sample_rate))

    logger.info("Storing raw data to {}..".format(dest_raw_bucket))

    source_signal_key = {'Bucket': source_bucket, 'Key': signal_key}
    source_metadata_key = {'Bucket': source_bucket, 'Key': metadata_key}

    raw_signal_key = "{}/{}/{}/{}/{}/{}/{}.wav.gz".format(switch_id, switch_turningdevice, switch_wire_num, event_starttime.year, event_starttime.month, event_starttime.day, event_id)
    raw_metadata_key = "{}/{}/{}/{}/{}/{}/{}.json".format(switch_id, switch_turningdevice, switch_wire_num, event_starttime.year, event_starttime.month, event_starttime.day, event_id)

    dest_s3_client = dest_session.client('s3', region_name=region)
    dest_s3_resource = dest_session.resource('s3', region_name=region)

    dest_s3_client.upload_file(local_temppath + temp_filename + ".wav.gz", dest_raw_bucket, raw_signal_key, ExtraArgs={'ACL': 'bucket-owner-full-control'})

    logger.info("- uploaded {}".format(raw_signal_key))

    dest_s3_resource.Object(dest_raw_bucket, raw_metadata_key).put(Body=metadata_content)
    logger.info("- wrote {}".format(raw_metadata_key))

    logger.info("Opening wav.gz..")

    with gzip.open(local_temppath + temp_filename + ".wav.gz", 'rb') as wav_signal:

        with wave.open(wav_signal, mode='rb') as wav: 
            
            n_samples = wav.getnframes()
            sample_freq = wav.getframerate()

            t_audio = n_samples/sample_freq

            logger.info("We have " + str(n_samples) + " samples from " + str(t_audio) + " seconds of signal")

            #########################################################################################################
            
            sample_too_long = False

            if (limit_sample and (t_audio > sample_max_length)):
                sample_too_long = True

            if sample_too_long:
                n_samples_before = n_samples
                t_audio_before = t_audio

                logger.info("Signal is over " + str(sample_max_length) + " seconds long.")    
                logger.info(f"Signal is too long and will be copied to failed inputs bucket with '{too_long_prefix}'-prefix")    

                debucket = s3.Bucket(debug_bucket)
                copy_source_gz = {'Bucket': source_bucket, 'Key': s3_file_name}
                copy_source_json = {'Bucket': source_bucket, 'Key': expected_json}
                logger.info(f"Archive bucket: {debug_bucket}")
                logger.info(f"Copy source: {copy_source_gz}")
                logger.info(f"Json source: {copy_source_json}")
                debucket.copy(copy_source_gz, too_long_prefix + s3_file_name, ExtraArgs={'ACL': 'bucket-owner-full-control'})
                debucket.copy(copy_source_json, too_long_prefix + expected_json, ExtraArgs={'ACL': 'bucket-owner-full-control'})

                logger.info("Files copied")

                logger.info("Using only first " + str(math.trunc(sample_max_length*sample_freq)) + " samples of " + str(n_samples) + " samples")
                n_samples = math.trunc(sample_max_length*sample_freq)
                t_audio = n_samples/sample_freq

                logger.info("We now have " + str(n_samples) + " samples from " + str(t_audio) + " seconds of signal")
                logger.info(str(n_samples_before - n_samples) + " samples and  " + str(t_audio - t_audio_before) + " seconds was cut off from the end of the signal")

            #########################################################################################################

            signal_wave = wav.readframes(n_samples)
            signal_array = np.frombuffer(signal_wave, dtype=np.int16)
            times = np.linspace(0, n_samples/sample_freq, num=n_samples)
            nframes = wav.getnframes()

            end_timestamp = event_starttime + pd.Timedelta(seconds=t_audio)

            timestamp_range = pd.date_range(start=event_starttime, 
                                            end=end_timestamp,
                                            periods=n_samples)

            df = pd.DataFrame(np.repeat(switch, n_samples), columns = ['switch']) # switch is main partition key

            df['event_starttime'] = event_starttime
            df['year'] = event_starttime.year # partition key
            df['month'] = event_starttime.month # partition key
            df['sampletime'] = timestamp_range
            df['sample_us'] = np.int64(times * 1000000)
            df['current'] = signal_array

            df['country_code'] = country_code
            df['location'] = location
            df['vayla_code'] = vayla_code
            df['switch_id'] = switch_id
            df['turningdevice'] = switch_turningdevice
            df['wire_num'] = switch_wire_num

            df['device_channel'] = device_channel
            df['device_code'] = device_code
            # df['device_configuration'] = device_configuration
            df['measurement_range'] = measurement_range

            df['bitrate'] = precalculated_bitrate
            df['duration'] = precalculated_duration
            df['energy'] = precalculated_energy
            df['max_current'] = precalculated_max_current
            df['median_current'] = precalculated_median_current
            df['min_current'] = precalculated_min_current
            df['sample_rate'] = precalculated_sample_rate

            # Signal can be plotted here for debug

            # plt.figure(figsize=(15, 5))
            # plt.plot(times, signal_array)
            # plt.ylabel('Signal')
            # plt.xlabel('Time (s)')
            # plt.xlim(0, t_audio)
            # plt.show()

            # Partitioning is done like this..
            # location/switch/turning device/year/month
            # e.q. HLT/V0231/C/2022/11/01

            # NOTE!! Change Windows-like /-path separators to \-backslashes when moving to Linux lambda

            local_partition_path = "{}.parquet/switch={}/year={}/month={}".format(switch,switch,event_starttime.year,event_starttime.month)
            dw_partition_path = "vaihde_signaali/switch={}/year={}/month={}".format(switch,event_starttime.year,event_starttime.month)

            # so, all the wires are stored in the same partition and also all days in a given month so that creating partition does not come an overhead

            logger.info("Writing local parquet-files in: " + local_temppath + local_partition_path + "...")

            df.to_parquet(local_temppath + switch + '.parquet', partition_cols=['switch','year','month'], engine='pyarrow', compression='snappy', coerce_timestamps='us', allow_truncated_timestamps=True)

            # Copy this folder structure recursively to DW in destination S3
            parquet_files = os.listdir(local_temppath + local_partition_path)

            # S3 Client for writing to target account    
            dest_s3_client = dest_session.client('s3', region_name=region)

            total_count = 0
            success_count = 0
            final_response = True
            # Loop all created files, upload them to destination and remove the local file 
            for pf in parquet_files:
                
                total_count += 1
                response = False
                retries = 10

                dw_key = dw_partition_path + "/" + pf

                try:
                    # Check if pf is a valid parquet file
                    logger.info("Checking the parquet file...")
                    check_parquet_file = pq.ParquetFile(local_temppath + local_partition_path + "/" + pf)
                    logger.info("OK. " + pf + " is a valid parquet file.")

                    while(response==False and retries>0):
                        try:
                            logger.info("Trying to upload parquet file to S3...")
                            dest_s3_client.upload_file(local_temppath + local_partition_path + "/" + pf, dest_bucket, dw_key, ExtraArgs={
                                'ACL': 'bucket-owner-full-control'
                            })      
                            logger.info("- uploaded local file: " + pf + " as " + dw_key)
                            try:
                                logger.info("Checking if the file is still a valid parquet file after the upload...")
                                test_object = dest_s3_client.get_object(Bucket=dest_bucket, Key=dw_key)
                                last_bits = test_object['Body'].read()[-4:]
                                logger.info(f"Last bits: {last_bits}")
                                if str(last_bits) == "b'PAR1'":
                                    logger.info("Last bits OK")
                                    logger.info("content legth: " + str(test_object['ContentLength']))
                                    if test_object['ContentLength'] >213:
                                        logger.info("Length > 213. Good. All seems to be in order")
                                        response = True
                                        success_count += 1
                                else:
                                    logger.error("Last bits don't match with the parquet magic number")
                            except Exception as e:
                                response = False
                                logger.error("The file is corrupted: " + str(e))

                        except ClientError as e:
                            response = False
                            logger.error("FAILED TO WRITE PARQUET: " + str(e))

                        retries -= 1

                        if (response==False and retries>0):
                            logger.info(f"Let's try again ({retries} attempts left)...")
                        elif (response == False and retries<=0):
                            final_response = False
                            logger.error(f"The uploaded file was corrupted. Trying to remove the file from {dest_bucket}")
                            try:
                                dest_s3_resource.Object(dest_bucket, dw_key).delete()
                                logger.info(f"Removed file {dw_key} from {dest_bucket}")
                            except Exception as e:
                                logger.error("File removal failed: " + str(e))
            
                except Exception as e:
                    response = False
                    final_response = False
                    logger.error("NOT VALID PARQUET: " + str(e))

            
            logger.info(f"{success_count}/{total_count} of parquet files were succesfully uploaded.")
            if final_response or success_count>0:
                logger.info("Creating athena partition...")

                athena = dest_session.client('athena', region_name=region)

                sql = "ALTER TABLE vaihde_signaali ADD IF NOT EXISTS PARTITION (switch = '{}', year='{}', month='{}')".format(switch,event_starttime.year,event_starttime.month)

                logger.info("Updating partitions...")

                # TODO: get database "vaihdedata-dev" and athena temp bucket from config
                try:
                    ExecuteAthenaQueryWithRetry(athena, "Analytiikkapilvi", athena_database, "analytiikka-prod-athena-temp", sql, retries = 10)
                except Exception as e:
                    logger.error("Updating paritions failed: " + str(e))
                    final_response = False

            # Do not make the partition if the writing parquet fails
            else:
                logger.info("Since all uploads failed, athena partition will not be created.")
            
            if not final_response:
                logger.info("Archiving source files for debugging purposes...")
                
                
                debucket = s3.Bucket(debug_bucket)
                copy_source_gz = {'Bucket': source_bucket, 'Key': s3_file_name}
                copy_source_json = {'Bucket': source_bucket, 'Key': expected_json}
                logger.info(f"Archive bucket: {debug_bucket}")
                logger.info(f"Copy source: {copy_source_gz}")
                logger.info(f"Json source: {copy_source_json}")
                debucket.copy(copy_source_gz, debug_prefix + s3_file_name, ExtraArgs={'ACL': 'bucket-owner-full-control'})
                debucket.copy(copy_source_json, debug_prefix + expected_json, ExtraArgs={'ACL': 'bucket-owner-full-control'})

                logger.info("Files copied")

            # Remove local files

            logger.info("Removing local files...")

            try:
                os.remove(local_temppath + temp_filename + ".wav.gz")
                shutil.rmtree(local_temppath + local_partition_path)
            except Exception as e:
                logger.error('Failed to remove local files: ' + e.__str__())
    
            # print("Testing debug bucket permissions. Uploading json to " + debug_bucket)
            # source_s3_resource.Bucket(debug_bucket).copy({'Bucket': source_bucket, 'Key': expected_json}, "test/" + expected_json)
