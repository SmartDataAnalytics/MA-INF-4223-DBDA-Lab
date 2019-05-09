COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

Lab Distributed Big Data Analytics

Worksheet-4: **SANSA**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen/), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu/),  [Denis Lukovnikov](http://sda.cs.uni-bonn.de/people/denis-lukovnikov/), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann/)

May 2, 2019

In this lab we are going to perform SANSA operations (described on “SANSA”).
In this lab, you will use SANSA to find out the subject distribution over nt file. The purpose is to demonstrate how to use the SANSA using Spark.

* * *

IN CLASS

* * *

1. SANSA
    - After a file ([page\_links\_simple.nt.bz2(http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2)) have been downloaded, unzipped, and uploaded on HDFS under /yourname folder you may need to create an RDD out of this file.
    - Read a RDF file by using SANSA and retrieve a Spark RDD representation of it.
    - Compute class distribution using the SANSA API and SANSA stats afterwords.
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
2. Read and explore
    - SANSA Overview and SANSA FAQ.
    - [SANSA-Examples](https://github.com/SANSA-Stack/SANSA-Examples).
    - [SANSA-Notebooks](https://github.com/SANSA-Stack/SANSA-Notebooks) with the existing examples.

3.  Further readings
    - [Distributed Semantic Analytics using the SANSA Stack](http://jens-lehmann.org/files/2017/iswc_sansa.pdf) by Jens Lehmann, Gezim Sejdiu, Lorenz Bühmann, Patrick Westphal, Claus Stadler, Ivan Ermilov, Simon Bin, Muhammad Saleem, Axel-Cyrille Ngonga Ngomo and Hajira Jabeen in Proceedings of 16th International Semantic Web Conference – Resources Track (ISWC’2017), 2017.
    - [The Tale of Sansa Spark](http://jens-lehmann.org/files/2017/iswc_pd_sansa.pdf) by Ivan Ermilov, Jens Lehmann, Gezim Sejdiu, Lorenz Bühmann, Patrick Westphal, Claus Stadler, Simon Bin, Nilesh Chakraborty, Henning Petzka, Muhammad Saleem, Axel-Cyrille Ngomo Ngonga, and Hajira Jabeen in Proceedings of 16th International Semantic Web Conference, Poster & Demos, 2017.