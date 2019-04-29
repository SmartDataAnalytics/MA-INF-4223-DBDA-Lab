COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

**Lab Distributed Big Data Analytics**

Worksheet-3: **ML on Spark (Spark ML and BigDL)**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen/), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu/),  [Denis Lukovnikov](http://sda.cs.uni-bonn.de/people/denis-lukovnikov/), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann/)

April 25, 2019

In this lab we are going to perform basic Spark ML and BigDL operations (described on “Spark Fundamentals II (ML on Spark)”).

* * *

IN CLASS

* * *

1.  Setup
    - Download Spark 2.2, unpack to `/opt/spark` (or anywhere)
    - Set `SPARK_HOME` var to `/opt/spark` (or where it was unpacked to)
    - Download BigDL 0.7, unpack anywhere
    - Set `BIGDL_HOME` var to unpacked BigDL directory
    - `do pip install bigdl==0.7` somewhere
    - download https://gist.github.com/lukovnikov/461d1165ea04317d2be6b66995ffa73c
    - start jupyter using the script (must be marked as executable)
2. Implement [PySpark-BigDL dummy linreg notebook](WorkSheet-3-notebooks/pyspark_bigdl_dummy_linreg_empty.ipynb).
3. Implement [PySpark-BigDL mnist notebook](WorkSheet-3-notebooks/pyspark_bigdl_mnist_empty.ipynb).
4. Implement [PySpark-BigDL mnist cnn notebook](WorkSheet-3-notebooks/pyspark_bigdl_mnist_cnn_empty.ipynb).

* * *

AT HOME

* * *

1.  Reading:
    - Read “Pattern Recognition and Machine Learning” by Bishop
    - Read “Deep Learning” by Courville et al. (or check some blog posts/tutorials)
    - Check out the MLlib programming guide
    - Read the [BigDL whitepaper](https://github.com/intel-analytics/BigDL/blob/master/docs/docs/whitepaper.md)
    - Check out the BigDL programming guide
    - Check out the tutorials (https://github.com/intel-analytics/BigDL-Tutorials/ ← Python)
1. Complete [the notebooks](WorkSheet-3-notebooks/)
2. Convert [the mnist_cnn notebook](WorkSheet-3-notebooks/pyspark_bigdl_mnist_cnn_empty.ipynb) to use [MLlib’s Pipeline API](https://spark.apache.org/docs/latest/ml-pipeline.html)
