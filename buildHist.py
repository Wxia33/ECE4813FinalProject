import csv
import numpy as np
import matplotlib.pyplot as plt

revCSV = 'review100.csv'
review = 'yelp_dataset/review.csv'
usrCSV = 'user100.csv'

with open(review) as csvFile:
    csvRead = csv.reader(csvFile, delimiter=',')
    usrDictAvgLen = {}
    usrStar = {}
    usrUseful = {}
    usrFunny = {}
    usrCool = {}
    prevUs = 0
    usrNum = 0
    for row in csvRead:
        if prevUs == 0:
            prevUs = row[1]
            continue
        if row[1] != prevUs:
            usrNum += 1
        prevUs = row[1]
        if usrNum in usrDictAvgLen:
            usrDictAvgLen[usrNum].append(len(row[5]))
            usrStar[usrNum].append(int(row[3]))
            usrUseful[usrNum] += int(row[6])
            usrFunny[usrNum] += int(row[7])
            usrCool[usrNum] += int(row[8])
        else:
            usrDictAvgLen[usrNum] = [len(row[5])]
            usrStar[usrNum] = [int(row[3])]
            usrUseful[usrNum] = int(row[6])
            usrFunny[usrNum] = int(row[7])
            usrCool[usrNum] = int(row[8])
    for usr in usrDictAvgLen:
        usrDictAvgLen[usr] = sum(usrDictAvgLen[usr]) / len(usrDictAvgLen[usr])
    for usrSt in usrStar:
        usrStar[usrSt] = sum(usrStar[usrSt]) / len(usrStar[usrSt])
    plt.bar(list(usrDictAvgLen.keys()), usrDictAvgLen.values())
    plt.xlabel('User Number')
    plt.ylabel('Average Review Length')
    plt.title('User Average Review Length')
    plt.show()

    plt.bar(list(usrStar.keys()), usrStar.values())
    plt.xlabel('User Number')
    plt.ylabel('Average Stars')
    plt.title('User Average Star Rating')
    plt.show()

    plt.bar(list(usrUseful.keys()), usrUseful.values())
    plt.xlabel('User Number')
    plt.ylabel('Total Useful Rating')
    plt.title('User Total Useful Rating')
    plt.show()

    plt.bar(list(usrFunny.keys()), usrFunny.values())
    plt.xlabel('User Number')
    plt.ylabel('Total Funny Rating')
    plt.title('User Total Funny Rating')
    plt.show()

    plt.bar(list(usrCool.keys()), usrCool.values())
    plt.xlabel('User Number')
    plt.ylabel('Total Cool Rating')
    plt.title('User Total Cool Rating')
    plt.show()

