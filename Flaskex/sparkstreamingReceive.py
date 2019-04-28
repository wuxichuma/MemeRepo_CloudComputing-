dataFrameUrl = "hdfs://gpu3:9000/dataFrames/final8"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext

import datetime

if __name__ == "__main__":
    sc = SparkContext(appName="Streaming for Upload base64 by socket")
    ssc = StreamingContext(sc, 5)
    
    preTime = datetime.datetime.now()
    
    lines = ssc.socketTextStream("localhost", int(9999))
    lines.pprint()
    outputFile = "hdfs://gpu3:9000/Streaming/StreamingOutput"
    lines.saveAsTextFiles(outputFile)
    

    '''
    lineList = lines.slice( preTime , datetime.datetime.now() )

    for line in lineList:
        print(" ========= lines.collect() =========== ", line.collect() )
        print(" ========= type( lines.collect() ) ============== ", type( line.collect() ) )

        uploadedDF = sc.parallelize( [ ("fromStreaming",line.collect()) ]).toDF(["path","binary"])
        uploadedDF.write.mode('append').parquet(dataFrameUrl)
    '''

    ssc.start()
    ssc.awaitTermination()

