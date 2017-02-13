# Partition (W)ARC Files by MIME Type and Year

This Hadoop/MapReduce tool splits and partitions Web archive records in (W)ARC(.gz) files by their MIME type and year.

It runs in **two phases**:

1. *Partition:* Files are split up and records are partitioned into files without headers of arbitrary sizes, depending on the input. This ia a map only job.
2. *Merge:* The partitioned files are merged into valid (W)ARC files with headers of fixed sizes with block size set two the file size.

It uses the [HadoopConcatGz](https://github.com/helgeho/HadoopConcatGz) input format to read the compressed (W)ARC files.

The first (partitioning) phase takes as **input** raw (W)ARC files on HDFS, while the second (merging) phase requires a text file with the paths of the files to process, one per line. The splitting of this file decides the number of mappers used and can be controlled by -Dmapreduce.input.lineinputformat.linespermap=1000 (for a 1000 lines/files per mapper). Such a text file can be created as follows:
```
hadoop fs -ls /path/to/partitioned/output/*/* | awk '{print $8}' > input_paths.txt
```
or, for only a specific partition (here, HTML in 2000):
```
hadoop fs -ls /path/to/partitioned/output/html/2000 | awk '{print $8}' > input_paths.txt
```
Then copy this *input_paths.txt* file to HDFS with ```hadoop fs -copyFromLocal input_paths.txt```.

To **control the partitions** to be created, please change the mapping in the [WarcPartitionMapper](src/main/java/de/l3s/warcpartitioner/WarcPartitionMapper.java). More **configurations** can be done by changing the constants in [WarcPartitioner](src/main/java/de/l3s/warcpartitioner/WarcPartitioner.java) and [WarcMerger](src/main/java/de/l3s/warcpartitioner/WarcMerger.java).
  
To **build** the required JAR file, just run ```mvn package```.

Finally, the jobs can be **run** as follows (with 8GB of memory in the partitioning phase):

1. *Partition:* ```hadoop jar warcpartitioner.jar de.l3s.warcpartitioner.WarcPartitioner -Dmapreduce.map.memory.mb=8192 -Dyarn.app.mapreduce.am.resource.mb=8192 /path/to/archive/*arc.gz /path/to/partitioned/output```
2. *Merge:* ```hadoop jar warcpartitioner.jar de.l3s.warcpartitioner.WarcMerger -Dmapreduce.input.lineinputformat.linespermap=1000 input_paths.txt /path/to/merged/output```

## License

The MIT License (MIT)

Copyright (c) 2017 Helge Holzmann (L3S)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
