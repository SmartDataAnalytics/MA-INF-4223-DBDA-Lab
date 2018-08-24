Completeness Aware Rule Learning
================================
## Description
Mining in KGs have high degree of incompleteness, which may provide inaccurate quality of mined rules, The effort of this algorithm is to expose that incompleteness by introducing aware scoring functions. First the algorithm count number of triples per relations and number of entities, then count the number of missing triples per relation and compute support and other metric for each possible rule and body. Based on ideas described in [this intuitive publication](https://www.researchgate.net/publication/320204461_Completeness-Aware_Rule_Learning_from_Knowledge_Graphs)

## Usage

Please configure it to work with Scala 2.11.x
```sh
git clone https://github.com/vinay1460n/CARL-KG.git
```
1. Open eclipse.
2. Click File > Import.
3. Type Maven in the search box under Select an import source:
4. Select Existing Maven Projects.
5. Click Next.
6. Click Browse and select the folder that is the root of the Maven project (probably contains the `pom.xml` file)
7. Click OK.

OR

This project also contains `build.sbt`.
- Install SBT
    ```sh
    echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 
    sudo apt-get update sudo apt-get install sbt
- In the root folder of the repository execute the following commands.
	```sh
	sbt compile
	sbt run
	```	
	Note: If you face memory issues or GC overhead errors increase the virtual machine's memory by setting and running the following command.
	```sh
	env JAVA_OPTS="-Xmx[MAX_HEAP_SPACE]m" sbt run 
	```
	Replace MAX_HEAP_SPACE with memory you want to increase. MAX_HEAP_SPACE depends on your input size and the system's available memory


