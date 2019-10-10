For the code to run we need the following packages installed:

Spark 2.2
BigDl 0.7
Python 3.6

We used google colab environment to develop the project as we were facing some issues with spark.

We can create python notebook here : https://colab.research.google.com/

3 easy steps below will help run the code:
	
	1. Mount the drive that has the input data folder using following code:
	
		# Run this cell to mount your Google Drive.
		from google.colab import drive
		drive.mount('/content/drive')
	
	As soon as we run this code, an url is displayed and once we click it it ask for permission and generates authorization code that we should enter in the password block displayed and click enter.
	
	2. Install pyspark using the command:
		
		!pip install pyspark==2.2.3
		
	3. Install bigdl using the command:
	
		!pip install bigdl==0.7


- 3 python files:

1. pre_processing.py - used for the data preprocessing

Data is already preprocessed(for both freebase and wordnet) and saved in the folder 'data'

2. model_with_loss.py - Actual model built and this outputs the scores of the triples(both positive & negative). Also outputs the Margin ranking loss
	 
	 To run this only input we should give is the input text field that has the triples in the line 39 of the code:
	 
	 triples_rdd = sc.textFile("/content/drive/My Drive/Colab Notebooks/data/FB15k/embedded_triples_train.txt")

3. model_with_training.py - This has the model with training optimizer and validation. This code has some issues and not being able to run it.
	 
	 To run this only input we should give is the input text field that has the triples in the line 39 of the code:
	 
	 triples_rdd = sc.textFile("/content/drive/My Drive/Colab Notebooks/data/FB15k/embedded_triples_train.txt")


