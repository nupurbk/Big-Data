import numpy as np
from sklearn import svm, grid_search
import cPickle
import time

startTime = time.time()

data= np.genfromtxt("part-r-00000", delimiter="\t")
labels = data[:,-1]
data= np.delete(data, (-1),axis =1)     #to delete the labels
#data = np.delete(data, (-1), axis=1)    #to delete the f t values
data= np.delete(data, (0),axis=1)       #to delete the listing id
print data.shape

parameters = {'kernel':('linear', 'rbf'), 'C':[1, 10, 100]}
svr = svm.SVC()
clf = grid_search.GridSearchCV(svr, parameters)
clf.fit(data, labels)
print "Best parameters: ",clf.best_params_
print "Best estimator: ",clf.best_estimator_
#with open('my_SVM_file.pkl', 'wb') as fid:
#    cPickle.dump(clf, fid)

#testdata= np.genfromtxt("AustinDataChanged.csv", delimiter=",")
#print clf.predict(testdata)

print ("**** %s seconds ****" %(time.time() - startTime))
