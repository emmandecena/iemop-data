import urllib.request
import urllib.error
import shutil
import os
import re
import zipfile
import logging
import datetime

logging.basicConfig(filename='test.log', level=logging.DEBUG, force=True)

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

class UnexpectedFormat(Exception):
    def __init__(self, message):
        self.message = message


def download(date, end, destdir):
    """Downloads daily RTD data from IEMOP website
    Example:
        download("2020-10-05","2020-10-05","/Volumes/data/projects/iemop-data")
    """
    try:
        base_url = "http://www.iemop.ph/market-data/rtd-prices-and-schedules/"
        url = "{}?post=5777&sort=&page=1&start={}%2000:00&end={}%2023:01".format(
            base_url, date, end
        )
        filename = os.path.join(destdir, "{}.zip".format(date))
        # Download the file from `url` and save it locally under `file_name`:
        with urllib.request.urlopen(url) as response, open(filename, "wb") as out_file:
            shutil.copyfileobj(response, out_file)
        return filename
    except urllib.error.HTTPError:
        print("Data not yet available")


def verify_ingest(outfile):
    expected_header = "RUN_TIME,MKT_TYPE,TIME_INTERVAL,REGION_NAME,RESOURCE_NAME,RESOURCE_TYPE,SCHED_MW,LMP,LOSS_FACTOR,LMP_SMP,LMP_LOSS,LMP_CONGESTION"

    with open(outfile, "r") as outfp:
        firstline = outfp.readline().strip()

        if firstline != expected_header:
            os.remove(outfile)
            msg = "Got header={}, but expected={}".format(firstline, expected_header)
            logging.ERROR(msg)
            raise UnexpectedFormat(msg)
        else:
            print("File {} verified".format(outfile))


def verify_dir(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            verify_ingest(os.path.join(directory, filename))
            continue
        else:
            print("Directory contains misc files, please remove")


def extract_nested_zip(zippedfile, tofolder):
    """Extract a zip file including any nested zip files
    Delete the zip file(s) after extraction
    Example:
    extract_nested_zip('/Volumes/data/projects/iemop-data/2020-10-05.zip','/Volumes/data/projects/iemop-data/')
    """
    try:
        with zipfile.ZipFile(zippedfile, "r") as zfile:
            zfile.extractall(path=tofolder)
        os.remove(zippedfile)

        for root, dirs, files in os.walk(tofolder):
            for filename in files:
                if re.search(r"\.zip$", filename):
                    filespec = os.path.join(root, filename)
                    extract_nested_zip(filespec, root)
    except FileNotFoundError:
        pass
