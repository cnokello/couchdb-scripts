from couchdbkit import Server, Database
from couchdbkit.loaders import FileSystemDocsLoader
from csv import DictReader
import sys, subprocess, math, os

def parseDoc(doc):
    print 'Parsing a document...'
    for k,v in doc.items():
        if v.isdigit() == True:
            doc[k] = int(v)
        else:
            try:
                if math.isnan(float(v)) == False:
                    doc[k] = float(v)
            except:
                pass
    print 'Parsing completed successfully'
    return doc

def upload(db, docs):
    print 'Uploading a document...'
    db.bulk_save(docs)
    del docs
    print 'Uploading successful'
    return list()

def uploadFile(fname, uri, dbname):
    print 'Upload contents of %s to %s/%s' % (fname, uri, dbname)

    # Connect to the database
    theServer = Server(uri)
    db = theServer.get_or_create_db(dbname)

    # Loop on file for upload
    reader = DictReader(open(fname, 'rU'), dialect = 'excel')

    # For bulk upload
    docs = list()
    checkpoint = 100
    
    for doc in reader:
        # Convert strings that are really numbers into ints and floats
        newdoc = parseDoc(doc) 
        
        # Check if doc already exists in the DB
        # If it already exists, update it
        #if db.doc_exist(newdoc.get('_id')):
        #    newdoc['_rev'] = db.get_rev(newdoc.get('_id'))

        docs.append(newdoc)

        if len(docs) % checkpoint == 0:
            docs = upload(db, docs)

    # Upload the lasr batch
    docs = upload(db, docs)

if __name__ == '__main__':
    print 'Reading parameters'
    filename = sys.argv[1]
    print 'File passed: %s\n' % filename
    uri = sys.argv[2]
    print 'Server URI: %s\n' % uri
    dbname = sys.argv[3]
    print 'Database name: %s\n' % dbname

    uploadFile(filename, uri, dbname)
