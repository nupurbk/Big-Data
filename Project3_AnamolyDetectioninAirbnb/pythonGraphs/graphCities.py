import matplotlib.pyplot as plt
import numpy as np

City = []
fraud = []
with open('Result_fraud.txt', 'r') as fp:
    for line in fp:
        Entries= line.split("\t")
        City.append(Entries[0])
        fraud.append(Entries[1])


fraud = np.array(map(float,fraud))
print "City is: ", City
print "fraud is: ", fraud

x=np.array(range(len(City)))
plt.xticks(x, City)
width = 1/1.9
plt.ylabel("Fraud Listings")

plt.bar(x, fraud, width, color= "gray")
plt.show()