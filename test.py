class DataUnavailable(Exception):
    def __init__(self, message):
    self.message = message

class UnexpectedFormat(Exception):
    def __init__(self, message):
    self.message = message

def verify_ingest(outfile):
    expected_header = "RUN_TIME, TIME_INTERVAL, RESOURCE_NAME, REGION_NAME, RESOURCE_TYPE, FUEL_TYPE, ZONE_ID, PARTICIPANT_ID, MW, LMP, LOSS_FACTOR, LMP_SMP, LMP_LOSS, LMP_CONGESTION"

    with open(outfile, "r") as outfp:
        firstline = outfp.readline().strip()
        if firstline != expected_header:
            os.remove(outfile)
            msg = "Got header={}, but expected={}".format(firstline, expected_header)
            logging.error(msg)
            raise UnexpectedFormat(msg)

        if next(outfp, None) == None:
            os.remove(outfile)
            msg = "Received file from BTS with only header and no content"
            raise DataUnavailable(msg)

def upload(csvfile, bucketname, blobname):
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blob = Blob(blobname, bucket) blob.upload_from_filename(csvfile)
    gcslocation = 'gs://{}/{}'.format(bucketname, blobname) print ('Uploaded {} ...'.format(gcslocation))
    return gcslocation

def ingest(date, bucket):
    ''' ingest flights data from BTS website to Google Cloud Storage return cloud-storage-blob-name on success.
    raises DataUnavailable if this data is not on BTS website
    '''
    tempdir = tempfile.mkdtemp(prefix='ingest_flights')
    try:
        zipfile = download(date, tempdir)
        bts_csv = zip_to_csv(zipfile, tempdir)
        verify_ingest(csvfile) # throws exceptions
        gcsloc = 'flights/raw/{}'.format(os.path.basename(csvfile)) return upload(csvfile, bucket, gcsloc)
    finally:
        print ('Cleaning up by removing {}'.format(tempdir))
        shutil.rmtree(tempdir)
