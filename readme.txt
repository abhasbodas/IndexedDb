A simple database implementation that takes command line or csv file input and stores data using linear hash indices that support overflow buckets and optimize memory utilization and reclaiming.

For compiling:

javac MainClass.java
This compiles all the files in the given folder.
This reads the file which is passed in the input argument.

Building a Heap File:
java MainClass test.hf -i -b1 -b3 "<" inputFile.csv

java MainClass test.hf -b1

Note: We have used 4k block size for the linear hash.

Query:

test.hf -s3 = 1950 -s4 = 1.75 -p1 -p2 > ex_result.acsv


Performance Comparision:

We have the initial csv file which has been given on the course page as the example input. 
We multiplicated the entries 204 times and made an index on -b3.
Total number of entries in Heap File were 3060.

Equality query on indexed column: 	Response time:  80,912,077 nanoseconds
Equality query on non-indexed column:	Response time: 273,664,220 nanoseconds

For 3000 entries, hash resulted in performance improvement by a factor of approximately 3.4.

For measuring performance, our program gives run time in nanoseconds on the console everytime it is run.