import os
import datetime
import glob

def load_dates(path):
    """
    This script is used to load all the dates with pipeline processed. 
    
    -------------------------------------------------------------------
    path: string
        path of the pipeline data
    """
    dates = os.listdir(path)
    dates = [d for d in dates if len(d)==10]
    try:
        dates = sorted(dates, key=lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'), 
        reverse=True)
    except:
        raise Exception("Not all directories in the path are dates of observation.")
    return dates


def main(path="/export/gotodata1/jdl/storage/pipeline/", UT='UT4'):
    """
    This is the python script to create the feature table for processed 
    data from GOTO. The table is not cleaned and it has to be cleaned 
    manually.

    -------------------------------------------------------------------
    path: string
        path of the pipeline data
    UT: string
        which UT observations will be processed
    """
    if not os.path.isdir("./results"):
        os.mkdir("results")
    dates = load_dates(path)
    for d in dates:
        images = glob.glob(path + d + "/final*/*" + UT + "-median.fits")
        print("image")
        
    

if __name__ == "__main__":
    main()