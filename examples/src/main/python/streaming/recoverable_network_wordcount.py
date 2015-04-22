#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in text encoded with UTF8 received from the network every second.

 Usage: recoverable_network_wordcount.py <hostname> <port> <checkpoint-directory> <output-file>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
   data. <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
   <output-file> file to which the word counts will be appended

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`

 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/recoverable_network_wordcount.py \
        localhost 9999 ~/checkpoint/ ~/out`

 If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
 checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
 the checkpoint data.
"""
from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def createContext(host, port, outputPath):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    if os.path.exists(outputPath):
        os.remove(outputPath)
    sc = SparkContext(appName="PythonStreamingRecoverableNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines = ssc.socketTextStream(host, port)
    words = lines.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    def echo(time, rdd):
        counts = "Counts at time %s %s" % (time, rdd.collect())
        print(counts)
        print("Appending to " + os.path.abspath(outputPath))
        with open(outputPath, 'a') as f:
            f.write(counts + "\n")

    wordCounts.foreachRDD(echo)
    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: recoverable_network_wordcount.py <hostname> <port> "
              "<checkpoint-directory> <output-file>", file=sys.stderr)
        exit(-1)
    host, port, checkpoint, output = sys.argv[1:]
    ssc = StreamingContext.getOrCreate(checkpoint,
                                       lambda: createContext(host, int(port), output))
    ssc.start()
    ssc.awaitTermination()
