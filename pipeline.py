from StringIO import StringIO
from PIL import Image
import numpy as np
import os, sys
from subprocess import Popen, PIPE
from snakebite.client import Client

if len(sys.argv) == 0:
    print("Usage: python pipeline INPUT_DIR OUTPUT_DIR HDFS_SERVER HDFS_PORT")
    exit(1)
HDFS_SERVER = sys.argv[3]
HDFS_PORT = sys.argv[4]
client = Client(HDFS_SERVER, int(HDFS_PORT), use_trash=False)
inputDir = sys.argv[1]
tmpOutDir = '/tmp/output'
outputDir = sys.argv[2]

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
        rootDir = tmpOutDir
        directory = rootDir + '/' + classname
        if not os.path.exists(directory):
            os.makedirs(directory)
        path = directory + '/' + key.split("/")[-1]
        im = Image.fromarray(img)
        im = im.resize(size, Image.ANTIALIAS)
        im.save(path)
        try:
            list(client.ls([outputDir + '/' + classname]))
        except:
            list(client.mkdir([outputDir + '/' + classname]))
        put = Popen(["hadoop", "fs", "-put" , path, outputDir + '/' + classname + '/' + key.split("/")[-1]])
        put.communicate()
    return img

from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "3g")
         .set('spark.driver.memory','15g'))
sc = SparkContext(conf = conf)

images = sc.binaryFiles('hdfs://' + HDFS_SERVER + ':' + HDFS_PORT + '/' + inputDir+'/*')
images.map(lambda (x, y): (x,transformAndSave(x, y))).collect()
sc.stop()
