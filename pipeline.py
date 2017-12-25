from StringIO import StringIO
from PIL import Image
import numpy as np
import os, sys

if len(sys.argv) == 0:
    print("Usage: python pipeline INPUT_DIR OUTPUT_DIR")
    exit(1)

inputDir = sys.argv[1]
outputDir = sys.argv[2]

if not os.path.exists(inputDir):
    print("Input Directory not available")
    exit(1)

if not os.path.exists(outputDir):
    os.makedirs(outputDir)

size = 300, 300

def toNPImage(data):
    try:
        return np.asarray(Image.open(StringIO(data)))
    except:
        return None

def transformAndSave(key, img):
    img = toNPImage(img)
    if img is not None:
        filename = key.split("/")[-1]
        classname = key.split("/")[-2]
        rootDir = outputDir
        directory = rootDir + '/' + classname
        if not os.path.exists(directory):
            os.makedirs(directory)
        print(len(os.listdir(directory)))
        path = directory + '/' + key.split("/")[-1]
        im = Image.fromarray(img)
        im = im.resize(size, Image.ANTIALIAS)
        im.save(path)
    return img

from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "3g")
         .set('spark.driver.memory','15g'))
sc = SparkContext(conf = conf)

images = sc.binaryFiles(inputDir + '/Bear')
count_of_all = images.count()
imagerdd = images.map(lambda (x, y): (x,transformAndSave(x, y))).collect()
