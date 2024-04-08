#Standard Libraries
import datetime
import hashlib
import time

#Third Party Libraries
from cloudant.client import Cloudant
import cloudant.database
import cloudant.document
import cloudant.query

# @DEV Return all documents in Cloudant DB with their respective Publisher Names and RSS_Feeds.
# @Param _database is a Cloudant db instance
# @Param _param_dictionary contains various parameters used to modify Cloudant json documents 
def get_all_docs(_database, _param_dictionary):
    docs_as_dictionaries = []
    doc_list = cloudant.query.Query(
        _database,
        selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
        fields= ["_id", "Publisher_Name","RSS_Feeds"],
        sort= [{"_id": "asc"}]
        ).result
    for doc in doc_list:
        docs_as_dictionaries.append(doc)
    return {"Status": "Success", "doc_list": docs_as_dictionaries}

# @DEV Remove a feed from a Publisher
# @Param _database is a Cloudant db instance
# @Param _param_dictionary contains various parameters used to modify Cloudant json documents
def delete_rss_feed(_database, _param_dictionary):
    deleted_object = {}
    rss_feed_name = ""
    
    # Delete the feed object from the publisher document
    with cloudant.document.Document(_database, document_id=_param_dictionary['collection_id']) as document:
        for doc_map in document['RSS_Feeds']:
            if doc_map['_id'] == _param_dictionary['rss_feed_id']:
                rss_feed_name = doc_map['RSS_Feed_Name']
                deleted_object = document['RSS_Feeds'].pop(document['RSS_Feeds'].index(doc_map))
                break
    
    # If the RSS feed has a respective no_pubdates doc, delete it
    if rss_feed_name + '_no_pubdates' in _database:
        #Delete the related no_pubdates feed document from the database
        with cloudant.document.Document(_database, rss_feed_name + '_no_pubdates') as doc:
            doc['_deleted'] = True
    
    return {"Status": "Success", "deleted": deleted_object, "deleted_no_pubdate" : rss_feed_name + '_no_pubdates'}

# @DEV Insert a new feed into a Publisher's list of feeds.
# @Param _database is a Cloudant db instance
# @Param _param_dictionary contains various parameters used to modify Cloudant json documents
def add_new_rss_feed(_database, _param_dictionary):
    #define new feed object
    new_feed_object = {
        "_id": hashlib.md5((_param_dictionary['publisher_name'] + _param_dictionary['rss_feed_name']).encode('utf-8')).hexdigest(),
        "RSS_Feed_Name": _param_dictionary['rss_feed_name'],
        "RSS_Feed_URL": _param_dictionary['rss_feed_url'],
        "Last_Updated_Date": datetime.datetime.today().strftime('%a, %d %b %Y %H:%M:%S %z'),
        "Threshold": _param_dictionary['threshold'],
        "Magazine": _param_dictionary['magazine']
    }
    if "language_feed" in _param_dictionary:
        new_feed_object["Language"] = _param_dictionary["language_feed"]
    if "pause_ingestion" in _param_dictionary:
        new_feed_object["Pause_Ingestion"] = _param_dictionary["pause_ingestion"]

    #If application supplied a collection_id, then we will add the feed to the existing document,
    #Else, we will create a new document and add the feed there instead.
    if not _param_dictionary['collection_id']:
        data = {
            "Publisher_Name": _param_dictionary['publisher_name'],
            "RSS_Feeds" : [
                new_feed_object
            ]
        }

        #check to see if a document with the same publisher already exists and append it, else create a new one
        doc_list = cloudant.query.Query(
        _database,
        selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
        fields= ["_id", "Publisher_Name","RSS_Feeds"],
        sort= [{"_id": "asc"}]
        ).result
        
        doc_found = False
        for doc in doc_list:
            if doc['Publisher_Name'] == _param_dictionary['publisher_name']:
                doc_found = doc
                break

        if not doc_found:
            # Create a document using the Database API
            rss_feed_doc = _database.create_document(data)

            if rss_feed_doc.exists():
                return {"Status" : "Success"}
            else:
                return {"Status" : "Failure"}
        else:
            #append to found 
            with cloudant.document.Document(_database, doc_found['_id']) as document:
                document['RSS_Feeds'].append(new_feed_object)
            return {"Status" : "Success"}

        
    else:
        doc_list = cloudant.query.Query(
        _database,
        selector= {"_id": {"$eq": _param_dictionary['collection_id']}},
        fields= ["_id", "Publisher_Name","RSS_Feeds"],
        sort= [{"_id": "asc"}]
        ).result

        #Open document up for editing
        for doc in doc_list:
            with cloudant.document.Document(_database, doc['_id']) as document:
                document['RSS_Feeds'].append(new_feed_object)
        return {"Status" : "Success"}

# @DEV Modify the data for a particular Publisher and Feed object
# @Param _database is a Cloudant db instance
# @Param _param_dictionary contains various parameters used to modify Cloudant json documents
def update_rss_feed(_database, _param_dictionary):

    if _param_dictionary['old_publisher_name'].lower() != _param_dictionary['publisher_name'].lower():
        doc_list = cloudant.query.Query(
        _database,
        selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
        fields= ["_id", "Publisher_Name","RSS_Feeds"],
        sort= [{"_id": "asc"}]
        ).result
        
        doc_id = ""
        doc_map = {}
        #find the json doc with the old_publisher name
        for doc in doc_list:
            #check if publisher exists
            if doc['Publisher_Name'].lower() == _param_dictionary['publisher_name'].lower():
                doc_id = doc['_id']
                break
                #print(doc_id)
            for doc_object in doc['RSS_Feeds']:
                if doc_object['_id'] == _param_dictionary['rss_feed_id']:
                    doc_map = doc_object
                    break
                    #print(doc_map)
                
        #if publisher was found, then add the rss_feed object to the new publisher name and remove it from the old
        if doc_id:
            new_feed_object = {
                "_id": hashlib.md5((_param_dictionary['publisher_name'] + _param_dictionary['rss_feed_name']).encode('utf-8')).hexdigest(),
                "RSS_Feed_Name": _param_dictionary['rss_feed_name'],
                "RSS_Feed_URL": _param_dictionary['rss_feed_url'],
                "Last_Updated_Date": doc_map['Last_Updated_Date'],
                "Threshold": _param_dictionary['threshold'],
                "Magazine": _param_dictionary['magazine']
            }
            if "language_feed" in _param_dictionary:
                new_feed_object["Language"] = _param_dictionary["language_feed"]
            if "pause_ingestion" in _param_dictionary:
                new_feed_object["Pause_Ingestion"] = _param_dictionary["pause_ingestion"]
            old_rss_feed_name = ""
            with cloudant.document.Document(_database, doc_id) as document:
                document['RSS_Feeds'].append(new_feed_object)
            with cloudant.document.Document(_database, _param_dictionary['collection_id']) as document:
                if document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]['RSS_Feed_Name'] !=  _param_dictionary['rss_feed_name']:
                    old_rss_feed_name = document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]['RSS_Feed_Name']
                del document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]
            # Rename the no_pubdate doc if the RSS feed name has changed and transport the data
            if old_rss_feed_name and (old_rss_feed_name + '_no_pubdates' in _database):
                update_no_pubdate_docs(_database, old_rss_feed_name, _param_dictionary['rss_feed_name'])
        #if publisher wasn't found, then delete the rss_feed object from orig document
        #and create a new document with the new publisher name and rss feed data
        else:
            old_rss_feed_name = ""
            with cloudant.document.Document(_database, _param_dictionary['collection_id']) as document:
                for doc_object in document['RSS_Feeds']:
                    if doc_object['_id'] ==  _param_dictionary['rss_feed_id']:
                        if doc_object['RSS_Feed_Name'] != _param_dictionary['rss_feed_name']:
                            old_rss_feed_name = doc_object['RSS_Feed_Name']
                        del document['RSS_Feeds'][document['RSS_Feeds'].index(doc_object)]
            _param_dictionary['collection_id'] = ""
            add_new_rss_feed(_database, _param_dictionary)
            # Rename the no_pubdate doc if the RSS feed name has changed and transpor the data
            if old_rss_feed_name and (old_rss_feed_name + '_no_pubdates' in _database):
                update_no_pubdate_docs(_database, old_rss_feed_name, _param_dictionary['rss_feed_name'])
    else:
        
        #Open document up for editing
        #for doc in doc_list:
        old_rss_feed_name = ""
        with cloudant.document.Document(_database, _param_dictionary['collection_id']) as document:
            for doc_map in document['RSS_Feeds']:
                if doc_map['_id'] == _param_dictionary['rss_feed_id']:
                    # Check if the RSS Feed Name has changed. If it did, we have to rename its respective no_pubdate doc
                    if doc_map['RSS_Feed_Name'] != _param_dictionary['rss_feed_name']:
                        old_rss_feed_name = doc_map['RSS_Feed_Name']
                    document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]['RSS_Feed_Name'] = _param_dictionary['rss_feed_name']
                    document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]['RSS_Feed_URL'] = _param_dictionary['rss_feed_url']
                    document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]['Threshold'] = _param_dictionary['threshold']
                    document['RSS_Feeds'][document['RSS_Feeds'].index(doc_map)]['Magazine'] = _param_dictionary['magazine']
        # Rename the no_pubdate doc if the RSS feed name has changed and transport the data
        if old_rss_feed_name and (old_rss_feed_name + '_no_pubdates' in _database):
            update_no_pubdate_docs(_database, old_rss_feed_name, _param_dictionary['rss_feed_name'])

    return {"Status" : "Success"}

# @DEV: Helper function to move data from old RSS Feed no_pudates file to the new one
# @PARAM _database is an instance of the IBM Cloudant DB
# @PARAM _old_rss_feed_name is what the Feed used to be named
# @PARAM _new_rss_feed_name is what the Feed name is being changed to
def update_no_pubdate_docs(_database, _old_rss_feed_name, _new_rss_feed_name):
    data = {}
    with cloudant.document.Document(_database, document_id = _old_rss_feed_name + '_no_pubdates') as old_doc:
        for key in old_doc.keys():
            data[key] = old_doc[key]
        old_doc['_deleted'] = True
            
    del data['_id']
    del data['_rev']

    with cloudant.document.Document(_database, document_id = _new_rss_feed_name + '_no_pubdates' ) as new_doc:
        new_doc['_id'] = _new_rss_feed_name + '_no_pubdates'
        for data_item in data.keys():
            new_doc[data_item] = data[data_item]

def main(_param_dictionary):
    client = Cloudant.iam(_param_dictionary['cloudant_endpoint'],_param_dictionary['api_key'],connect=True)
    database = cloudant.database.CloudantDatabase(client, _param_dictionary['db_name'])   
    switch_map = {
        "get" : get_all_docs,
        "delete" : delete_rss_feed,
        "update" : update_rss_feed,
        "add": add_new_rss_feed
    }
    result = switch_map[_param_dictionary['action']](database, _param_dictionary)

    return {
          "headers": {
              "Content-Type": "application/json",
          },
          "statusCode": 200,
          "body": result,
      }