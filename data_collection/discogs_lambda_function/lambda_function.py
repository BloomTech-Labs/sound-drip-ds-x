import json
import boto3
from datetime import datetime


def lambda_handler(event, context):
    """
    Scheduled task to retrieve new "masters" uploads from discogs s3 bucket
    and download them to account s3 bucket

        Parameters:
            event (json) : unused
            context (?) : unused
    
        Returns:
            json{'status': xxx, 'body': xxx}
    """
    
    year = datetime.today().year
    
    #Specifying bucket and file variables
    from_bucket_name = 'discogs-data'
    from_bucket_prefix = f'data/{year}'
    
    to_bucket_name = 'dscg-data'
    to_bucket_prefix = f'{year}-zips'
    
    from_index_file = f'{year}-index.txt'
    to_index_file = f'/tmp/{year}-index.txt'
    
    #Resources represent an object-oriented interface to Amazon Web Services (AWS)
    s3 = boto3.resource('s3')

    from_bucket = s3.Bucket(from_bucket_name)
    to_bucket = s3.Bucket(to_bucket_name)
    
    #Download index file, this documents old/unwanted/already-processed files 
    to_bucket.download_file(from_index_file, to_index_file)
    
    try:
        #Reference for unwanted files
        already_files = []
        
        with open(to_index_file, 'r') as index_file:
            for line in index_file:
                already_files.append(line.rstrip())    # rstrip removes '\n' character from string
                
    except Exception as err:
        print(err)
        return {
        'statusCode': 500,
        'body': json.dumps(f"Error reading {to_index_file}")
        }
  
    
    #Creates s3.ObjectSummary for bucket ie- lists its contents and metadata
    discog_files_to_dl = from_bucket.objects.filter(Prefix=from_bucket_prefix)
    
    for obj in discog_files_to_dl:
        file_obj = obj.key.split('/')[-1]    #Removing prefix path, root file eg - data/2020/some_file.zip -> some_file.zip
        if 'masters' in file_obj:            #Only want 'masters' files
            if file_obj not in already_files:    #Only want new files
                
                try:
                    #Write new files to index file so next evocation knows it has been processed
                    with open(to_index_file, 'a') as index_file:
                        index_file.write('\n')      #Ensure newline
                        index_file.write(file_obj)
                
                except Exception as err:
                    print(err)
                    return {
                    'statusCode': 500,
                    'body': json.dumps(f"Error writing to {to_index_file}")
                    }
                    
                #Object name and bucket reference  
                from_source = {'Bucket': from_bucket_name, 
                              'Key': obj.key}
    
                new_key = obj.key.replace(from_bucket_prefix, to_bucket_prefix, 1)   #Formatting for put prefix(subfolder) 
                new_obj = to_bucket.Object(new_key)    #Create new object in target bucket
                new_obj.copy(from_source)    #Copy object from source bucket
                    
    #Upload updated index file
    s3_client = boto3.client('s3')
    s3_client.upload_file(to_index_file, to_bucket_name, from_index_file)            
    
    
    return {
        'statusCode': 200,
        'body': json.dumps("success")
    }
