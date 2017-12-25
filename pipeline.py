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

'''
This function converts Binary data read from HDFS to image data for processing.
Also rejecting malformed and invalid images.
'''

def toNPImage(data):
    try:
        return np.asarray(Image.open(StringIO(data)))
    except:
        return None

'''
This function does the transform and saving of file back to hdfs
This can be optimized by partitioning data after the whole resizing is done.
maybe a pipeline of first scaling and then paritioning into training, test
and validation makes sense.

Also assumptions are that the invalid and malformed files are way less then
valid images to not screw up the calculation of paritions
'''

def transformAndSave(key, img):
    img = toNPImage(img)
    if img is not None:
        classname = key.split("/")[-2]
        try:
            list(client.ls([outputDir + '/' + classname]))
        except Exception as ex:
            for dirs in ['testing', 'validation', 'training']:
                list(client.mkdir([outputDir + '/' + classname + '/' + dirs], True))
        currentNumFiles = int(list(client.count([outputDir + '/' + classname]))[0]['fileCount'])
        currentNumFiles += 1
        totalInClass = int(list(client.count([inputDir + '/' + classname]))[0]['fileCount'])
        filename = "img" + str(currentNumFiles) + ".jpg"
        rootDir = tmpOutDir
        directory = rootDir + '/' + classname
        if not os.path.exists(directory):
            os.makedirs(directory)
        path = directory + '/' + filename
        im = Image.fromarray(img)
        im = im.resize(size, Image.ANTIALIAS)
        im.save(path)
        midDir = 'training'
        if currentNumFiles * 100/totalInClass >= 70:
            midDir = 'testing'
        elif currentNumFiles * 100/totalInClass >= 90:
            midDir = 'validation'
        put = Popen(["hadoop", "fs", "-put" , path, outputDir + '/' + classname + '/' + midDir + '/'])
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
