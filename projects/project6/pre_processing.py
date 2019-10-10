import numpy as np
import json

'''
Removing unwanted data and spaces from the triples and generating clean triples
Input: train.txt
Output: train_preprocessed.txt
'''
outfile = open("/content/drive/My Drive/Colab Notebooks/data/FB15k/train_preprocessed.txt", 'a+')
with open("/content/drive/My Drive/Colab Notebooks/data/FB15k/train.txt") as infile:
    for line in infile:
      split_lines = line.split('\t')
      e1 = split_lines[0][3:]
      e2 = split_lines[1][3:]
      r = split_lines[2].split('/')[-1]
      outfile.write(e1)
      outfile.write('\t')
      outfile.write(e2)
      outfile.write('\t')
      outfile.write(r)
infile.close()
outfile.close()

'''
Preprocessing of entitiy2id and relation2id files
Input: entity2id.txt
Output: entity2id_preprocessed.txt
'''
outfile = open("/content/drive/My Drive/Colab Notebooks/data/FB15k/entity2id_preprocessed.txt", 'a+')
with open("/content/drive/My Drive/Colab Notebooks/data/FB15k/entity2id.txt") as infile:
    for line in infile:
      split_lines = line.split('\t')
      e1 = split_lines[0][3:]
      id = split_lines[1]
      #r = split_lines[2].split('/')[-1]
      outfile.write(e1)
      outfile.write('\t')
      outfile.write(id)
      #outfile.write('\t')
      #outfile.write(r)
infile.close()
outfile.close()


'''
This code creates a common one-hot index encoding of each entity and relation and store them in a json file 'ent_id.json'
Once this is done, We take the train_preprocessed.txt file and encode the triples into one-hot indexes and store them in the final input file
embedded_triples_train.txt. This file will be used as the input for the model. We have done the preprocesing and the now the model will directly use the preprocessed data from embedded_triples_train.txt

'''
final_dict = {}
ent_dict = {}
rel_dict = {}
id_list = []
with open('/content/drive/My Drive/Colab Notebooks/data/FB15k/ent_id.json', 'w', encoding='utf-8') as outfile:
    with open("/content/drive/My Drive/Colab Notebooks/data/FB15k/entity2id_preprocessed.txt") as infile:    
        for line in infile:
          split_lines = line.split('\t')
          entity = split_lines[0]
          ide = split_lines[1].strip()
          if entity not in ent_dict.keys():
              ent_dict[entity] = float(ide) + 1.0
              id_list.append(float(ide) + 1.0)
        final_dict['entities'] = ent_dict 
        num_ent = float(ide) + 1.0
    with open("/content/drive/My Drive/Colab Notebooks/data/FB15k/relation2id_preprocessed.txt") as ifile:
        for lines in ifile:
          split_line = lines.split('\t')
          relation = split_line[0]
          idr = 14951.0 + float(split_line[1]) + 1.0
          if relation not in rel_dict.keys():
              rel_dict[relation] = idr
              id_list.append(idr)
        final_dict['relations'] = rel_dict
    json.dump(final_dict, outfile, ensure_ascii=False, indent=2) 
outfile.close()
infile.close()
with open('/content/drive/My Drive/Colab Notebooks/data/FB15k/embedded_triples_train.txt', 'w', encoding='utf-8') as embdfile:
    with open("/content/drive/My Drive/Colab Notebooks/data/FB15k/train_preprocessed.txt") as trnfile:
      for trip in trnfile:
        triple = trip.split('\t')
        embdfile.write(str(final_dict['entities'][triple[0]]))
        embdfile.write('\t')
        embdfile.write(str(final_dict['relations'][triple[2].strip()]))
        embdfile.write('\t')
        embdfile.write(str(final_dict['entities'][triple[1]]))
        embdfile.write('\n')
embdfile.close()
trnfile.close()