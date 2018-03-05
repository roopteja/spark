import os
import sys
import datetime
os.environ['SPARK_HOME']="/home/roopteja/spark-1.6.1"
os.environ['PYSPARK_PYTHON']=sys.executable
try:
    from pyspark import SparkContext, SparkConf
    print("Successfully imported Spark modules")
except ImportError as e:
    print("Error Importing Spark Modules", e)
    sys.exit(1)
try:
   import numpy
   print("Successfully imported Numpy Module")
except ImportError as e:
   print("Error Importing Numpy Module", e)
   sys.exit(1)
try:
   from scipy import misc,sum
   print("Successfully imported Scipy module : misc")
except ImportError as e:
   print("Error Importing Scipy Modules", e)
   sys.exit(1)

startDate = datetime.datetime.strptime(sys.argv[3],"%Y-%m-%d %H-%M-%S")
endDate = datetime.datetime.strptime(sys.argv[4],"%Y-%m-%d %H-%M-%S")
day = sys.argv[2]
location = sys.argv[1]

conf = SparkConf().setAppName("ServerApp")
sc = SparkContext(conf=conf)
filerdd = sc.binaryFiles("hdfs://roopteja:54310/user/trafficCamData/"+location+"/"+day+"/*.jpg")

def filterImages(imgFile):
    date = datetime.datetime.strptime(imgFile[0], "hdfs://roopteja:54310/user/trafficCamData/"+location+"/"+day+"/"+location+"%Y-%m-%d %H-%M-%S.jpg")
    if startDate < date < endDate:
        return True
    return False
def loadImages(imgFile):
    from PIL import Image
    from StringIO import StringIO
    img = Image.open(StringIO(imgFile[1]))
    rgb = numpy.asarray(img,dtype=numpy.uint8)
    return rgb    
def subtract(imgfile):
    val = numpy.subtract(median.value,imgfile)
    m_norm = numpy.float_(sum(abs(val)))
    result = m_norm/median.value.size
    return str(result)
imagesrdd = filerdd.filter(filterImages).map(loadImages)
medianImg = numpy.median(imagesrdd.collect(),axis=0).astype(numpy.uint8)
misc.imsave("/home/roopteja/results/"+sys.argv[3]+"median.jpg", medianImg)
median = sc.broadcast(medianImg.astype(numpy.int16))
text = imagesrdd.map(subtract).collect()
with open('/home/roopteja/results/'+sys.argv[3]+'json.txt','w') as f:
    f.write(",".join(text))
