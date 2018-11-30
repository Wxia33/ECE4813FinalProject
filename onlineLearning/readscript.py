import csv
import json
import sqlite3
from sqlite3 import Error


def readjson(filename):
	data = []
	with open(filename, encoding="utf8") as file:
		for line in file:
			data.append(flattenjson(json.loads(line),"_"))
			#data.append(json.loads(line))
	for x in data:
		for y in x:
			if '{' in y:
				#print("{ check")
				data.remove(x)
	return data

def writecsv(filename, inlist):
    keys = inlist[0].keys()
    with open(filename, 'w', encoding = "utf8", newline='') as mycsv:
        dictwriter = csv.DictWriter(mycsv, keys)
        dictwriter.writeheader()
        dictwriter.writerows(inlist)
        #mywriter.writerows(inlist)
    mycsv.close()

def flattenjson(b, delim):
    val = {}
    for i in b.keys():
        if isinstance( b[i], dict ):
            get = flattenjson( b[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = b[i]
    return val

def cleanbusiness(inlist):
	newlist = []
	for indict in inlist:
		rmlist = []
		for x in indict:
			if 'attributes' in x or 'hours' in x:
				rmlist.append(x)
		for x in rmlist:
			indict.pop(x, None)
		newlist.append(indict)
	return newlist
'''
def cleanreview(inlist):
	newlist = []
	for indict in inlist:
		rmlist = []
		for x in indict:
			if 'attributes' in x or 'hours' in x:
				rmlist.append(x)
		for x in rmlist:
			indict.pop(x, None)
		newlist.append(indict)
	return newlist
'''

def main():
    #business = cleanbusiness(readjson('yelp_dataset/yelp_academic_dataset_business.json'))
    #review = readjson('yelp_dataset/yelp_academic_dataset_review.json')
    user = readjson('yelp_dataset/yelp_academic_dataset_user.json')
    #writecsv('yelp_dataset/business.csv',business)
    #writecsv('review100.csv',review)
    writecsv('user100.csv',user)

if __name__ == "__main__":
    main()

