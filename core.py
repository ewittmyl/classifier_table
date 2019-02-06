import os
import datetime
import glob
from subprocess import Popen


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


def prep(image_path, image):
    """
    This is used to copy the image to the present directory for 
    processing. Including funpack the .fz file.
    ------------------------------------------------------------
    image_path: string
        input path for the image
    
    image: string
        input image name
    """
    
    # run bash code with 'Popen'
    P = Popen('cp '+ image_path + ' ./', shell=True)
    P.wait()
    P = Popen('mv ' + image + '.fits ' + image + '.fits.fz', shell=True)
    P.wait()
    P = Popen('funpack *.fz', shell=True)
    P.wait()
    P = Popen('rm -rf *.fz', shell=True)
    P.wait()


def sex(image):
    """
    Re-run sextractor to generate necessary parameters for creating feature table.
    --------------------------------------------------------------------------------
    image: string
      input image name
    """
   
    # run sextractor with different default parameters
    print('running SExtractor to {}...'.format(image))
    P = Popen('sex -c goto1.sex ' + image + '.fits', shell=True)
    P.wait()
    P = Popen('psfex default.fits', shell=True)
    P.wait()
    P = Popen('sex -c goto2.sex -CATALOG_NAME ' + image + '.cat ' + image + '.fits', shell=True)
    P.wait()
    P = Popen('mv ' + image + '.cat results', shell=True)
    P.wait()


def remove_sideproducts(image=None):
    unwanteds = ['default.fits','default.psf','*png','chi_default.fits','resi_default.fits','samp_default.fits','psfex.xml','proto_default.fits','snap_default.fits']
    for unwanted in unwanteds:
        P = Popen('rm -rf ' + unwanted, shell=True)
        P.wait()
    if image:
        P = Popen('rm -rf ' + image + '.fits', shell=True)
        P.wait()

def merge_tables(path="./results/"):
    try:
        cat_list = os.listdir(path)
        cat_path = path + cat_list
        if (os.path.isfile("./detections.csv")) and (os.path.isfile("./merged_images.txt")):
            merged_table = pd.read_csv("./detections.csv")
            merged_records = open("./merged_images.txt",'a+')
            
        else:
            if (os.path.isfile("./detections.csv")) or (os.path.isfile("./merged_images.txt")):
                P = Popen("rm -rf detections.csv", shell=True)
                P.wait()
                P = Popen("rm -rf merged_images.txt", shell=True)
                P.wait()
            Popen("touch merged_images.txt", shell=True)
            merged_records = open("./merged_images.txt",'a+')
            merged_table = pd.concat([cat_path[0],cat_path[1]])
            merged_records.write(cat_list[0]+'\n'+cat_list[1]+'\n')
            
        merging = list(set(cat_list) & set([line.split('\n') for line in merged_records.readlines()]))
        for cat in merging:
            merged_table = pd.concat([merged_table, path+cat])
            merged_records.write(cat+'\n')
        cat.to_csv("detections.csv", index=False, header=False)
        merged_records.close()

    except KeyboardInterrupt:
        cat.to_csv("detections.csv", index=False, header=False)
        merged_records.close()
        raise KeyboardInterrupt




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
    try:
        if not os.path.isdir("./results"):
            os.mkdir("results")
        dates = load_dates(path)
        processed_img = os.listdir("./results")
        processed_img = [i.split(".")[0] for i in processed_img]
        for d in dates:
            print("Processing date: {}".format(d))
            images = glob.glob(path + d + "/final*/*" + UT + "-median.fits")
            for img_path in images:
                img = img_path.split("/")[-1].split(".")[0]
                if not img in processed_img:
                    prep(img_path, img)
                    sex(img)
                    remove_sideproducts(img)
                else:
                    pass
    except KeyboardInterrupt:
        remove_sideproducts()
        P = Popen('rm -rf *cat' , shell=True)
        P.wait()
        P = Popen('rm -rf *median.fits', shell=True)
        P.wait()

    

if __name__ == "__main__":
    main()