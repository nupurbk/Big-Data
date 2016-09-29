import numpy as np
import cPickle
import sys
from sklearn.metrics import mean_squared_error
from math import sqrt

#fileName = sys.argv[1]
fileName = 'part-r-00000-Austin'

with open('my_SVM_file.pkl','rb') as fid:
    clf = cPickle.load(fid)

NotFraud = 0
Fraud = 0
TruthLabel = []
ObtainedLabel = []
fw = open('Result_fraud.txt','a')
with open(fileName, 'r') as fp:
    for line in fp:
        line = line.replace("\t",",").split(",")
        listingId = line[0]
        line1 = map(int,line[1:6])
        TruthLabel.append(line[6])
        result = int(clf.predict(line1))
        ObtainedLabel.append(result)
        if result == 0:
            Fraud += 1
        else:
            NotFraud += 1
        string = str(listingId+"\t"+str(result)+"\n")

        #print "Listing Id:",listingId, "Result:",int(clf.predict(line1))

TruthLabel = map(int,TruthLabel)
rms = sqrt(mean_squared_error(TruthLabel, ObtainedLabel))*100
print "RMSE is: ", rms
print "Fraud entries : ", Fraud
print "Not Fraud entries: ", NotFraud

string = "SantaCruz\t"+str(Fraud)
fw.write(string)
fw.close()