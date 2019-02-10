import os, datetime, glob
from subprocess import Popen
import pandas as pd
import simplejson as sj


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
    P = Popen('sextractor -c goto1.sex ' + image + '.fits', shell=True)
    P.wait()
    P = Popen('psfex default.fits -c default.psfex', shell=True)
    P.wait()
    P = Popen('sextractor -c goto2.sex -CATALOG_NAME ' + image + '.cat ' + image + '.fits', shell=True)
    P.wait()
    tab = extra_features(image + '.cat')
    P = Popen('mv ' + image + '.csv results', shell=True)
    P.wait()
    P = Popen('rm -rf ' + image + '.cat', shell=True)


def remove_sideproducts(image=None):
    unwanteds = ['default.fits','default.psf','*svg','chi_default.fits','resi_default.fits','samp_default.fits','psfex.xml','proto_default.fits','snap_default.fits']
    for unwanted in unwanteds:
        P = Popen('rm -rf ' + unwanted, shell=True)
        P.wait()
    if image:
        P = Popen('rm -rf ' + image + '.fits', shell=True)
        P.wait()

def extra_features(table):
    print("Creating new features for {}".format(table.split(".")[0]))

    col = ["FLUX_APER2","FLUX_APER4","FLUX_APER5","FLUX_APER8","FLUX_APER10",
        "FLUX_APER14","MAG_APER2","MAG_APER4","MAG_APER5","MAG_APER8",
        "MAG_APER10","MAG_APER14","MAG_AUTO","MAG_PETRO","KRON_RADIUS",
        "PETRO_RADIUS","FLUX_MAX","ISOAREAF_IMAGE","X_IMAGE","Y_IMAGE",
        "X_WORLD","Y_WORLD","X2_IMAGE","Y2_IMAGE","XY_IMAGE","THETA_IMAGE",
        "X2WIN_IMAGE","Y2WIN_IMAGE","XYWIN_IMAGE","AWIN_IMAGE","BWIN_IMAGE",
        "THETAWIN_IMAGE","AWIN_WORLD","BWIN_WORLD","THETAWIN_WORLD","MU_MAX",
        "FLAGS","FWHM_IMAGE","ELONGATION","CLASS_STAR","FLUX_RADIUS25",
        "FLUX_RADIUS50","FLUX_RADIUS85","FLUX_RADIUS95","FLUX_RADIUS99",
        "SPREAD_MODEL","SPREADERR_MODEL"]

    tab = pd.read_table(table, skiprows=35, sep=r'\s+', header=None, names=col)
    
    '''create FWHM_MEAN and id column'''
    tab["FWHM_MEAN"] = tab["FWHM_IMAGE"].mean()
    tab["id"] = table.split("_")[0][1:]

    '''save to csv table'''
    tab.to_csv(table.split(".")[0]+'.csv', index=False, header=False)


def merge_tables(path="./results/"):
    try:
        col = ["FLUX_APER2","FLUX_APER4","FLUX_APER5","FLUX_APER8","FLUX_APER10",
        "FLUX_APER14","MAG_APER2","MAG_APER4","MAG_APER5","MAG_APER8",
        "MAG_APER10","MAG_APER14","MAG_AUTO","MAG_PETRO","KRON_RADIUS",
        "PETRO_RADIUS","FLUX_MAX","ISOAREAF_IMAGE","X_IMAGE","Y_IMAGE",
        "X_WORLD","Y_WORLD","X2_IMAGE","Y2_IMAGE","XY_IMAGE","THETA_IMAGE",
        "X2WIN_IMAGE","Y2WIN_IMAGE","XYWIN_IMAGE","AWIN_IMAGE","BWIN_IMAGE",
        "THETAWIN_IMAGE","AWIN_WORLD","BWIN_WORLD","THETAWIN_WORLD","MU_MAX",
        "FLAGS","FWHM_IMAGE","ELONGATION","CLASS_STAR","FLUX_RADIUS25",
        "FLUX_RADIUS50","FLUX_RADIUS85","FLUX_RADIUS95","FLUX_RADIUS99",
        "SPREAD_MODEL","SPREADERR_MODEL","FWHM_MEAN"]
        cat_list = os.listdir(path)
        cat_path = [path + c for c in cat_list]
        if (os.path.isfile("./detections.csv")) and (os.path.isfile("./merged_images.txt")):
            print("Detection table and merged record are loaded.")
            merged_table = pd.read_csv("./detections.csv",header=None)
            with open("merged_images.txt",'r') as f:
                merged_records = sj.load(f)
            merging = list(set(cat_list) - set(merged_records))

        else:
            if (os.path.isfile("./detections.csv")) or (os.path.isfile("./merged_images.txt")):
                print("Detection table or/and merged record is missing. Creating a new one...")
                P = Popen("rm -rf detections.csv", shell=True)
                P.wait()
                P = Popen("rm -rf merged_images.txt", shell=True)
                P.wait()
            Popen("touch merged_images.txt", shell=True)
            print("Merging {}".format(cat_list[0]))
            print("Merging {}".format(cat_list[1]))
            df0 = pd.read_table(cat_path[0],skiprows=35,sep=r'\s+',header=None,names=col)
            print(df0['FWHM_IMAGE'].mean())
            df0['FWHM_MEAN'] = df0['FWHM_IMAGE'].mean()
            df1 = pd.read_table(cat_path[1],skiprows=35,sep=r'\s+',header=None,names=col)
            print(df1['FWHM_IMAGE'].mean())
            df1['FWHM_MEAN'] = df1['FWHM_IMAGE'].mean()
            merged_table = pd.concat([df0, df1], ignore_index=True)
            merged_records = [cat_list[0], cat_list[1]]
            merging = cat_list[2:]

        for cat in merging:
            print("Merging {}".format(cat))
            df = pd.read_table(path+cat,skiprows=35,sep=r'\s+',header=None,names=col)
            print(df['FWHM_IMAGE'].mean())
            df['FWHM_MEAN'] = df['FWHM_IMAGE'].mean()
            merged_table = pd.concat([merged_table, df], ignore_index=True)
            merged_records.append(cat)
        merged_table.to_csv("detections.csv", index=False, header=False)
        with open('merged_images.txt','w') as f:
            sj.dump(merged_records, f)
        print("All tables are merged.")
        

    except KeyboardInterrupt:
        merged_table.to_csv("detections.csv", index=False, header=False)
        with open('merged_images.txt','w') as f:
            sj.dump(merged_records, f)
        raise KeyboardInterrupt("detection.csv is saved!")




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
    main("/mnt3/data/public/goto/commissioning/pipeline/")
