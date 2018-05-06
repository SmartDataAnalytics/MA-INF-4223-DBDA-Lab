COMPUTER SCIENCE DEPARTMENT, UNIVERSITY OF BONN

* * *

### Lab Distributed Big Data Analytics

Worksheet-1: **Setting up environment and getting started with Scala**

* * *

[Dr. Hajira Jabeen](http://sda.cs.uni-bonn.de/dr-hajira-jabeen), [Gezim Sejdiu](http://sda.cs.uni-bonn.de/gezim-sejdiu), [Prof. Dr. Jens Lehmann](http://sda.cs.uni-bonn.de/prof-dr-jens-lehmann)

April 17, 2018

Over the lab during the semester, we are going to use a variety of tools, including Apache Hadoop (HDFS)[\[1\]](#ftnt_ref1), Apache Spark[\[2\]](#ftnt_ref2), Docker[\[3\]](#ftnt_ref3) and many more. Installing this tools and getting started can often be a hassle, mainly because of the dependencies. You are allowed to choose which option : (i) on your own machine; (ii) using Virtualbox[\[4\]](#ftnt_ref4) and Docker; (iii) or via VMs, to use when you install these packages.

Tasks:

* **Virtualbox**: \- it is a virtualization software package similar to VMWare or other virtual tools. We will make use of it to setup and configure our working environment in order to complete assignments.
Here are the steps to be followed:
    - Download the latest Ubuntu ISO from[http://www.ubuntu.com/download/desktop](http://www.ubuntu.com/download/desktop) (use 64 bit).
    - Create a new virtual machine with options: Type = Linux, Version = Ubuntu (64 bit).
    -  Recommended memory size: 2GB
    -  Select: "Create a Virtual Hard Drive Now".
        -  Leave the setting for Hard Drive File Type unchanged (i.e., VDI).
        -  Set the hard drive to be "Dynamically Allocated".
        -  Size: ~10GB
    - The virtual machine is now created.
    - Press “**Start**”
        - Navigate to the Ubuntu ISO that you have downloaded, and Press Start.
        - On the Boot Screen: "Install Ubuntu"
        - Deselect both of "Download Updates while Installing" and "Install Third-Party Software"
        - Press “Continue”
        - Select "Erase disk and install Ubuntu"
        - Add your account informations:
        - Name = "yourname"; username = "username"; password = "****";
        - Select "Log In Automatically"
        - Press "Restart Now"

    - Log in to the machine.
        - Open the terminal (Ctrl + Alt + T) and execute these commands:
        - Download and upgrade the packages list
        ```sh
        sudo apt-get update
        sudo apt-get upgrade
        ```

    - Installing JAVA
        
        ```sh
        sudo add-apt-repository ppa:webupd8team/java
        sudo apt-get update  
        sudo apt-get install oracle-java8-installer
        ```

        \- Setting the JAVA_HOME Environment Variable
        ```sh
        sudo update-alternatives --config java
        sudo nano /etc/environment
        ```

        \- At the end of this file, add the following line, making sure to replace the highlighted path with your own copied path.
        ```sh
        JAVA_HOME="/usr/lib/jvm/java-8-oracle"
        source /etc/environment
        echo $JAVA_HOME
        ```

    - Install Maven
    ```sh
    sudo apt-get update
    sudo apt-get install maven
    ```

    - Install SBT (optional)
    ```sh
    echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 
    sudo apt-get update sudo apt-get install sbt
    ```

    - Install Hadoop (Single Node Setup)
        \- Creating a Hadoop user for accessing HDFS
        ```sh 
        sudo addgroup hadoop 
        sudo adduser --ingroup hadoop hduser
        ```

        \- Installing SSH
        ```sh
        sudo apt-get install openssh-server
        ```

        Configuring SSH
        ```sh 
        #First login with hduser (and from now use only hduser account for further steps).
        sudo su hduser

        # Generate ssh key for hduser account
        ssh-keygen -t rsa -P ""

        #Copy id_rsa.pub to authorized keys from hduser
        cat $HOME/.ssh/id\_rsa.pub >> $HOME/.ssh/authorized\_keys
        ```

        \- Installations Steps
        ```sh
        #Download latest Apache Hadoop source from Apache mirrors
        wget http://mirror.nohup.it/apache/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz
        
        # Extract Hadoop source
        tar xzf hadoop-2.8.3.tar.gz
        rm hadoop-2.8.3.tar.gz
        \## Move hadoop-2.8.3 to hadoop folder
        sudo mv hadoop-2.8.3 /usr/local
        sudo ln -sf /usr/local/hadoop-2.8.3/ /usr/local/hadoop

        #Assign ownership of this folder to Hadoop user./
        sudo chown -R hduser:hadoop /usr/local/hadoop-2.8.3/
        
        #Create Hadoop temp directories for Namenode and Datanode 
        sudo mkdir -p /usr/local/hadoop/hadoop_store/hdfs/namenode 
        sudo mkdir -p /usr/local/hadoop/hadoop_store/hdfs/datanode
        
        #Again assign ownership of this Hadoop temp folder to Hadoop user 
        sudo chown hduser:hadoop -R /usr/local/hadoop/hadoop_store/
        ```
        
        \- Update Hadoop configuration files
        ```sh
        #User profile : Update $HOME/.bashrc
        nano ~/.bashrc
        
        #Set Hadoop-related environment variables  
        export HADOOP_PREFIX=/usr/local/hadoop  
        export HADOOP_HOME=/usr/local/hadoop  
        export HADOOP\_MAPRED\_HOME=${HADOOP_HOME} 
        export HADOOP\_COMMON\_HOME=${HADOOP_HOME} 
        export HADOOP\_HDFS\_HOME=${HADOOP_HOME}  
        export YARN_HOME=${HADOOP_HOME}  
        export HADOOP\_CONF\_DIR=${HADOOP_HOME}/etc/hadoop
        
        #Native path  
        export HADOOP\_COMMON\_LIB\_NATIVE\_DIR=${HADOOP_PREFIX}/lib/native  
        export HADOOP\_OPTS="-Djava.library.path=$HADOOP\_PREFIX/lib/native" 
        
        #Java path  
        export JAVA_HOME="/usr/lib/jvm/java-8-oracle" 
        
        #Add Hadoop bin/ directory to PATH  
        export PATH=$PATH:$HADOOP\_HOME/bin:$JAVA\_PATH/bin:$HADOOP_HOME/sbin
        ```
        In order to have the new environment variables in place, reload .bashrc
        ```sh
        source ~/.bashrc
        ```

        \- Configure Hadoop
        ```sh
        cd /usr/local/hadoop/etc/hadoop
        nano yarn-site.xml
        ```
        ```yml
        <configuration>  
            <property>  
            <name>yarn.nodemanager.aux-services</name>  
            <value>mapreduce_shuffle</value>  
            </property>  
        </configuration>
        ```
        ```sh
        nano core-site.xml
        ```
        ```yml
        <configuration>  
            <property>
            <name>fs.defaultFS</name> 
            <value>hdfs://localhost:54310</value>  
            </property>  
        </configuration>
        ```
        ```sh
        cp mapred-site.xml.template mapred-site.xml
        nano mapred-site.xml
        ```
        ```yml
        <configuration>  
            <property>  
                <name>mapreduce.framework.name</name> 
                <value>yarn</value>  
            </property>
            <property>
                <name>mapred.job.tracker</name>
                <value>localhost:54311</value>
                <description>The host and port that the MapReduce job tracker runs at. If  local", then jobs are run in-process as a single map and reduce task.
                </description>
            </property>  
        </configuration>
        ```
        ```sh
        nano hdfs-site.xml
        ```
        ```yml
        <configuration> 
            <property>
                <name>dfs.replication</name>  
                <value>1</value> 
            </property>  
            <property>  
                <name>dfs.namenode.name.dir</name> 
                <value>file:/usr/local/hadoop/hadoop_store/hdfs/namenode</value>  
            </property>  
            <property>  
                <name>dfs.datanode.data.dir</name>  
                <value>file:/usr/local/hadoop/hadoop_store/hdfs/datanode</value> 
            </property>  
        </configuration>
        ```
    \- Finally, set to “/usr/lib/jvm/java-8-oracle” the JAVA_HOME variable in /usr/local/hadoop/etc/hadoop/hadoop-env.sh.
    
    \- Starting Hadoop
    ```sh
    sudo su hduser  
    hdfs namenode -format  
    start-dfs.sh  
    start-yarn.sh
    ```

    \- Create a directory on HDFS.
    ```sh 
    hdfs dfs -mkdir /user  
    hdfs dfs -mkdir /user/hduser
    ```
    \- Run a MapReduce job.hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.3.jar pi 10 50
    
    \- Track/Monitor/Verify
    ```sh
    jps
    ```
    For ResourceManager – [http://localhost:8088](http://localhost:8088)

    For NameNode – [http://localhost:50070](http://localhost:50070)
    
    Finally, to stop the hadoop daemons, simply invoke stop-dfs.sh and stop-yarn.sh.

    - Install Spark
    ```sh 
    mkdir $HOME/spark  
    cd $HOME/spark  
    wget http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz  
    tar xvf spark-2.2.0-bin-hadoop2.7.tgz  
    nano ~/.bashrc  
    export SPARK_HOME=$HOME/spark/spark-2.2.0-bin-hadoop2.7/  
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin  
    source ~/.bashrc  
    
    start-master.sh  
    start-slave.sh <master-spark-URL> 
    spark-shell --master <master-spark-URL>
    ```
    
    SparkMaster – [http://localhost:8080/](http://localhost:8080)
    
    Installing Scala
    ```sh
    wget https://downloads.lightbend.com/scala/2.11.11/scala-2.11.11.tgz  
    sudo tar xvf scala-2.11.11.tgz  
    nano ~/.bashrc  
    export SCALA_HOME=$HOME/scala-2.11.11/  
    export PATH=$SCALA_HOME/bin:$PATH  
    source ~/.bashrc  
    scala -version
    ```
    
    - Configure IDE with Scala and Spark
    
        Here are steps how to configure scala-ide for eclipse.
        - Go to [http://scala-ide.org/download/sdk.html](http://scala-ide.org/download/sdk.html) and download the version needed for your Linux 32 or 64-bit to your linux server/workstation. The latest version is 4.7.0.
        - Meet JDK requirements: JDK 8
        - Copy the archive to your preferred folder and decompress.
        - Find eclipse executable and execute it
        
        Please configure it to work with Scala 2.11.x
        
        After the scala-ide have been configured properly we could set up a spark-template project to get started with Scala and Spark.
        ```sh
        git clone https://github.com/SANSA-Stack/SANSA-Template-Maven-Spark.git
        ```
        1. Open eclipse.
        2. Click File > Import.
        3. Type Maven in the search box under Select an import source:
        4. Select Existing Maven Projects.
        5. Click Next.
        6. Click Browse and select the folder that is the root of the Maven project (probably contains the `pom.xml` file)
        7. Click OK.

Assignment:

* * *

IN CLASS

* * *

1.  Data Store & Processing using HDFS

    - Start HDFS and verify its status.  
    ```sh
    hadoop-daemon.sh start namenode  
    hadoop-daemon.sh start datanode  
    hdfs dfsadmin -report
    ```
    - Create a new directory `/youname` on HDFS.
    ```sh
    hdfs dfs -mkdir /gezim
    ```
    - Download [page\_links\_simple.nt.bz2](http://downloads.dbpedia.org/3.9/simple/page_links_simple.nt.bz2), unzip it, on your local filesystem and upload it to HDFS under /yourname folder.
    ```sh
    hdfs dfs -put page\_links\_simple.nt /gezim
    ```
    - View the content and the size of `/yourname` directory.
    ```sh
    hdfs dfs -ls -h /gezim/page\_links\_simple.nt
    ```
    - Copy the file just created on `/yourname` into `page\_links\_simple_hdfscopy.nt`
    ```sh
    hdfs dfs -cp /gezim/page\_links\_simple.nt /gezim/page\_links\_simple_hdfscopy.nt
    ```
    - Copy your file back to local filesystem and name it `page\_links\_simple_hdfscopy.nt`
    ```sh
    hdfs dfs -get /gezim/page\_links\_simple_hdfscopy.nt
    ```
    - Remove your file from HDFS.
    ```sh
    hdfs dfs -rm /gezim/page\_links\_simple.nt
    ```
    - Remove `/yourname` directory from HDFS.
    ```sh
    hdfs dfs -rm -r /gezim
    ```
1.  Basics of Scala

    Define a class Point which describes an (x, y) coordinate.
    ```scala
        class Point(val  x: Int, val  y: Int) extends App {           
        }
    ```
    Create a companion object Point such will allow you to instantiate Point without using new.
    ```scala object  Point {
        def  apply(x: Int, y: Int) = new Point(x, y)
        }
    ```
    Create a singleton object Origin that represents the (0, 0) coordinates of Point.
    ```scala 
        object  Origin  extends Point(0, 0)
    ```
    Instantiate the two object of Origin and check if both refer to the same object in memory.
    ```scala 
        val  p1 = Point
        val  p2 = Point
        println(p1.eq(p2))
    ```
    Implement a function distanceTo which calculates the distance between two Point instances.
    ```scala 
        def  distanceTo(other: Point): Double = {
            val  dx = math.abs(x  -  other.x)
            val  dy = math.abs(y  -  other.y)
            math.sqrt(math.pow(dx, 2) + math.pow(dy, 2))
        }
    ```

* * *

AT HOME

* * *

1.  Read and explore
    - Functional Programming
    - Recursion, and Tail recursion
    - Anonymous functions
    - High Order Functions
    - Currying
1.  Read a textfile and do a word count on that file. Hint: create and populate a Map with words as keys and counts of the number of occurrences of the word as values[\[5\]](#ftnt5).
    ```sh
    Hello Hello World
    (Hello, 2)
    (World, 1)
    ```

1.  Apply what you have read above by creating a function that returns sum of a range of integers by applying a user defined function to it.
```latex
    \sum\f_{i=a}^{b}
```
e.g. f=cube, and sum(2,4) would result in 2^3+3^3+4^3 .

1.  Further readings
    - [http://www.artima.com/scalazine/articles/steps.html](http://www.artima.com/scalazine/articles/steps.html)
    - [http://www.scala-lang.org/](http://www.scala-lang.org/)
    - [http://twitter.github.io/scala_school/](http://twitter.github.io/scala_school/)

* * *

[\[1\]](#ftnt_ref1)  [http://hadoop.apache.org/](http://hadoop.apache.org/)

[\[2\]](#ftnt_ref2)  [http://spark.apache.org/](http://spark.apache.org/)

[\[3\]](#ftnt_ref3)  [https://www.docker.com/](https://www.docker.com/)

[\[4\]](#ftnt_ref4)  [https://www.virtualbox.org/](https://www.virtualbox.org/)

[\[5\]](#ftnt_ref5)  [http://ampcamp.berkeley.edu/big-data-mini-course/introduction-to-the-scala-shell.html](http://ampcamp.berkeley.edu/big-data-mini-course/introduction-to-the-scala-shell.html)
