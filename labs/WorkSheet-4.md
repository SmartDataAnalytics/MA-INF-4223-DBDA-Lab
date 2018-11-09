COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

Lab Distributed Big Data Analytics

Worksheet-4: **Spark ML and SANSA**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen/), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu/), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann/)

November 8, 2018

In this lab we are going to perform basic Spark ML and SANSA operations (described on “Spark Fundamentals II (MLlib - GraphX)” and “SANSA”).

In this lab, you will use MLlib and SANSA to find out the subject distribution over nt file. The purpose is to demonstrate how to use the Spark MLlib and SANSA using Spark.

* * *

IN CLASS

* * *

1. Spark ML
    - After a file ([page\_links\_simple.nt.bz2](http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2)) have been downloaded, unzipped, and uploaded on HDFS under /yourname folder you may need to create an RDD out of this file.
    - First create a Scala class Triple containing information about a triple read from a file, which will be used as schema. Since the data is going to be type of .nt file which inside contains rows of triples in format <subject> <predicate> <object> we may need to transform this data into a different format of representation. Hint: Use map function.
    - Create an RDD of a Triple object
    - Use the filter transformation to return a new RDD with a subset of the triples on the file by checking if the first row contains “#”, which on .nt file represent a comment.
    - Implement **TF-IDF** algorithm for finding the most used classes on the given dataset.
        - TF-IDF in Spark uses
            - TF: HashingTF is a Transformer which takes sets of terms and converts those into fixed-length feature vectors.
            - IDF:IDF is an Estimator which fits on a dataset and produces an IDFModel which takes feature vectors and scales each column.
        - List classes into a separate RDD.
        - Split each label into words using Tokenizer. For each sentence we use Hashing TF to hash the sentence into a feature vector. And then use IDF to rescale the feature vectors.
        - Pass this feature vector into a learning algorithm.

    - Collect and print the results.

2.  SANSA
    - After a file ([page\_links\_simple.nt.bz2(http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2)) have been downloaded, unzipped, and uploaded on HDFS under /yourname folder you may need to create an RDD out of this file.
    - Read a RDF file by using SANSA and retrieve a Spark RDD representation of it.
    - Read an OWL file by using SANSA and retrieve a Spark RDD/DataSet representation of it.
    - Use SANSA-Inference layer in order to apply some inference/reasoning over RDF file by applying RDFS profile reasoner.

* * *

AT HOME

* * *

1.  SANSA-Notebook
    - Run [SANSA-Examples](https://github.com/SANSA-Stack/SANSA-Examples) using [SANSA-Notebooks](https://github.com/SANSA-Stack/SANSA-Notebooks) and perform this example:
        - Read an file into RDD representation
        - Apply Property Distribution and Class Distribution using SANSA functions
        - Use the same graph and apply RDFS reasoner using SANSA-Inference
        - Apply Class distribution to the inferred graph above (see iii)
        - Rank resources to the inferred graph and show top 20 most ranked entities.
    - Read and explore
        - Spark Machine Learning Library (MLlib) Guide
        - SANSA Overview and SANSA FAQ.

3.  Further readings
    - [MLlib: Machine Learning in Apache Spark](http://www.jmlr.org/papers/volume17/15-237/15-237.pdf)
    - [Distributed Semantic Analytics using the SANSA Stack](http://jens-lehmann.org/files/2017/iswc_sansa.pdf) by Jens Lehmann, Gezim Sejdiu, Lorenz Bühmann, Patrick Westphal, Claus Stadler, Ivan Ermilov, Simon Bin, Muhammad Saleem, Axel-Cyrille Ngonga Ngomo and Hajira Jabeen in Proceedings of 16th International Semantic Web Conference – Resources Track (ISWC’2017), 2017.
    - [The Tale of Sansa Spark](http://jens-lehmann.org/files/2017/iswc_pd_sansa.pdf) by Ivan Ermilov, Jens Lehmann, Gezim Sejdiu, Lorenz Bühmann, Patrick Westphal, Claus Stadler, Simon Bin, Nilesh Chakraborty, Henning Petzka, Muhammad Saleem, Axel-Cyrille Ngomo Ngonga, and Hajira Jabeen in Proceedings of 16th International Semantic Web Conference, Poster & Demos, 2017.