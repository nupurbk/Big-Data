import numpy as np
import cPickle
import sys

line = sys.argv[1]

line = map(int, line.split(","))

with open('hdfs://rio-grande:30162/dataLocTest/my_SVM_file.pkl' , 'rb') as fid:
    clf = cPickle.load(fid)

print clf.predict(line)
