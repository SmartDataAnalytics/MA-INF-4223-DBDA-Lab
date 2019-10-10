from pyspark import SparkContext, SparkConf
from bigdl.nn.layer import *
import numpy as np
from bigdl.optim.optimizer import Optimizer, Adam, MaxEpoch, MaxIteration, Adagrad
from bigdl.util.common import *
from bigdl.nn.criterion import *
import matplotlib.pyplot as plt
import random


init_engine()
# Model - Definition
def model_seq():
  model = Sequential()
  model = model.add(Reshape([2,3]))
  model = model.add(LookupTable(16500, 3, 2.0, 0.1, 2.0, True))
  #model = model.add(SplitTable(1, nInputDims = 2))
  model = model.add(SplitTable(2,3))
  branches = ParallelTable()
  branch1 = Sequential().add(Linear(3,3))
  branch3 = Sequential().add(Linear(3,3))
  branch2 = Sequential().add(Identity())
  branches.add(branch1).add(branch2).add(branch3)
  model = model.add(branches)
  model = model.add(DotProduct())
  model = model.add(SplitTable(1,1))
  #model = model.add(Bilinear(2,2,1))
  return model

#Margin Ranking Critetion 
mse = MarginRankingCriterion()
#Seting spark context
conf = SparkConf().setAppName("Read Text to RDD - Python")
sc = SparkContext.getOrCreate()

input_triples_final = []

# read input text file to RDD
triples_rdd = sc.textFile("/content/drive/My Drive/Colab Notebooks/data/FB15k/embedded_triples_train.txt")
triples = triples_rdd.take(10000)

for triple in triples:
  split_triple = triple.split('\t')
  input_triple = [float(split_triple[0]),float(split_triple[1]),float(split_triple[2]),float(random.randint(1,14500)),float(split_triple[1]),float(split_triple[2])]
  input_triples_final.append(input_triple)

input_triples_ndarray = np.array(input_triples_final).astype("float32")
print('input triples data type:',type(input_triples_ndarray))
print('input triples: ',input_triples_ndarray)
mod = model_seq()
scores_pos = []
scores_neg = []
loss = []
#Training
for evr_triple in input_triples_ndarray:
  input = np.array(evr_triple).astype("float32")
  #input1 = [input[0:3], input[3:0]]
  #print(input1)
  input_new = mod.forward(input)
  print('Model Output data type:', type(input_new))
  print('Model Output: ', input_new)
  #all_x = model_seq().forward(input)
  print('+ve triple score: ', input_new[0])
  scores_pos.append(input_new[0])
  scores_neg.append(input_new[1])
  print('-ve triple score: ', input_new[1])
  target1 = np.array([1]).astype("float32")
  target = [target1]
  train_data_rdd = sc.parallelize(zip(input_triples_ndarray, target1)).map(lambda x : Sample.from_ndarray(x[0], float(x[1])))
  print('Training input: ',train_data_rdd.take(2))

  # Loss Function

  output = mse.forward(input_new, target)
  print('Margin rank between the +ve triple & -ve triple is: ',output)
  loss.append(output)
  
plt.plot(scores_pos, color='g', label = 'pos')
plt.plot(scores_neg, color='orange', label = 'neg')
plt.xlabel('model scores')
plt.ylabel('value')
plt.title('Triple scores - +ve and -ve')
plt.legend()
plt.show()
plt.plot(loss, color='red')
plt.xlabel('loss')
plt.ylabel('value')
plt.title('Margin Ranking Loss')
plt.show()

# Create an Optimizer
optimizer = Optimizer(
    model=mod,
    training_rdd=train_data_rdd,
    criterion=MarginRankingCriterion(),
    optim_method=None,
    end_trigger=MaxEpoch(100),
    batch_size=10)


appname = 'DISTMULT'+ dt.datetime.now().strftime("%Y%m%d-%H%M%S")
train_summary = TrainSummary(log_dir='bigdl_summaries',
                                     app_name=app_name)

train_summary.set_summary_trigger("Parameters", SeveralIteration(50))
val_summary = ValidationSummary(log_dir='bigdl_summaries',
                                        app_name=app_name)

optimizer.set_train_summary(train_summary)
optimizer.set_val_summary(val_summary)

# Validation
batch_size = 10
validation_results = mod.evaluate(train_data_rdd, batch_size, [Loss()])
print(validation_results[0])