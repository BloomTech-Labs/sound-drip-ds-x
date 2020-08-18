import boto3
from datetime import datetime
import gzip
import json
import os
import xml.sax



class dscg_handler(xml.sax.ContentHandler):
        """ 
        Custom handler class for xml.sax

            Attributes:
                CurrentData (str) : name of the current tag being read
                
                discogs_parsed (dictionary) : aggregate data store for 
                                              artist/album/track
                
                na_number (int) : dummy variable counter to create unique 
                                  album refs in the case of an empty one
                
                album (string) : name of the album the content is being 
                                 read from in the tree
                
                artist (string) : name of the artist the contents is being
                                  read from in the tree

            Methods:
                startElement(self, tag, attributes):
                    saves element tag to CurrentData and resests album string when
                    starting a new one

                endElement(self, tag, attributes):
                    refreshes CurrentData before a new element is read and
                    creates a new jso file obj in s3 when the xml file has
                    reached the end
                
                characters(self, content):
                    performs operations based on CurrentData tag
        """


        def __init__(self):
            """
            Constructs necessary attributes for handler object

                Parameters:
                    CurrentData (str) : name of the current 
                                        tag being read
                    
                    discogs_parsed (dictionary) : aggregate data store for 
                                                  artist/album/track
                    
                    na_number (int) : dummy variable counter to create unique
                                      album refs in the case of an empty one
                    
                    album (string) : name of the album the content is being 
                                     read from in the tree
                    
                    artist (string) : name of the artist the contents is being
                                      read from in the tree
            """

            self.CurrentData = ""
            self.discogs_parsed = {}
            self.na_number = 0
            self.album = ""
            self.artist = ""
    
       
        def startElement(self, tag, attributes):
            """
            Saves element tag to CurrentData and resests album string when
            starting a new one

            Parameters:
                tag (str) : tag name of current element
                attributes : attributes of the tag 
                             eg: <masters id='120'>...<masters/> (attributes is id)
            """

            self.CurrentData = tag
            
            if tag == "master": # indicates new album
                self.album = ""
    
    
        def endElement(self, tag):
            """
            Refreshes CurrentData before a new element is read and creates a 
            new json file obj in target s3 when the xml file has reached the end

            Parameters:
                tag (str) : tag name of current element
            """
            
            if tag == 'masters':
                obj = s3.Object(to_bucket_name,
                                to_bucket_prefix + file_obj[:-6] + "json")
                
                obj.put(Body=json.dumps(self.discogs_parsed))
                
                self.discogs_parsed = {} # reset main dict to decrease memory usage? in case of multi-uploads

            self.CurrentData = ""
    
        
        def characters(self, content):
            """
            Performs operations with content based on CurrentData tag name

            Parameters:
                content (*?) : data inside the element tag
            """
            
            if self.CurrentData == "name": # indicates artist
                self.artist = content
                if self.artist in self.discogs_parsed.keys(): # check if already exists
                    pass
                else:
                    self.discogs_parsed[self.artist] = {} # create nested dict in new
             
    
            elif self.CurrentData == 'title' and self.album == '': # indicates new album
                self.album = content
                
                if not self.album: # check for blank content, ran into this before with manual testing
                    self.album = f"not_available_{self.na_number}" # create unique (to this file) album name
                    self.na_number += 1
                
                self.discogs_parsed[self.artist][self.album] = [] # create array for artist/album keys to hold tracks
            
            
            elif self.CurrentData == "title" and len(self.album) > 0: # title is also used to signify tracks inside album, if album is not empty, then its a track
                self.discogs_parsed[self.artist][self.album].append(content)



def lambda_handler(event, context):
    """
    Scheduled task to retrieve new "masters" uploads from discogs s3 bucket,
    read in the .gz file, parse it to a json-like object(nested dicts), and
    upload that object to our account s3 

        Parameters:
            event (json) : unused
            context (?) : unused
    
        Returns:
            json{'status': xxx, 'body': xxx} : unused
    """
    global s3, to_bucket_name, to_bucket_prefix, file_obj # making these variable accesible to dscg_handler
    
    year = datetime.today().year
    
    # specifying bucket and file variables
    from_bucket_name = 'discogs-data'
    from_bucket_prefix = f'data/{year}'
    
    to_bucket_name = 'dscg-data'
    to_bucket_prefix = f'{year}-parsed/'
    
    from_index_file = f'{year}-index.txt'
    to_index_file = f'/tmp/{year}-index.txt'
    
    # resources represent an object-oriented interface to Amazon Web Services (AWS)
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')

    from_bucket = s3.Bucket(from_bucket_name)
    to_bucket = s3.Bucket(to_bucket_name)
        
    # download index file, this documents old/unwanted/already-processed files
    to_bucket.download_file(from_index_file, to_index_file)
    
    try:
        # reference for unwanted files
        already_files = []
        
        with open(to_index_file, 'r') as index_file:
            for line in index_file:
                already_files.append(line.rstrip()) # rstrip removes '\n' character from string
                
    except Exception as err:
        print(err)
        raise(err)
  
    # creates s3.ObjectSummary for bucket ie- lists its contents and metadata
    discog_files_to_dl = from_bucket.objects.filter(Prefix=from_bucket_prefix)
    multi_file_ref = None # varible switch if multiple files need uploaded
    
    for obj in discog_files_to_dl:
        file_obj = obj.key.split('/')[-1] # removing prefix path, root file eg - data/2020/some_file.zip -> some_file.zip
        if 'masters' in file_obj:
            if file_obj not in already_files:
                
                try:
                    # write new file  names to index file so next evocation knows it has been processed
                    with open(to_index_file, 'a') as index_file:
                        index_file.write('\n') # ensure newline
                        index_file.write(file_obj)
                
                except Exception as err:
                    print(err)
                    raise(err)
                
                
                try:
                    
                    if multi_file_ref: # if more than one file needs parsed, delete old .gz file in memory
                        os.remove(multi_file_ref)
                        
                    file_path = '/tmp/' + file_obj
                    s3_client.download_file(from_bucket_name, obj.key, file_path)
                    multi_file_ref = file_path
                
                except Exception as err:
                    print(err)
                    raise(err)
                    
               
                parser = xml.sax.make_parser()
                parser.setFeature(xml.sax.handler.feature_namespaces, 0)
                parser.setContentHandler(dscg_handler())

                try:
                    # initiate parsing
                    with gzip.open(file_path,'r') as fp:
                        parsed = parser.parse(fp)
                
                except Exception as err:
                    print(err)
                    raise(err)

                
                    
    # upload updated index file
    s3_client = boto3.client('s3')
    s3_client.upload_file(to_index_file, to_bucket_name, from_index_file)    
    
    return {
            'status': 200,
            'body' : 'success'
    }