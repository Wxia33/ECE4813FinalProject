import csv

def weightedMajorityAlgorithm(filename):
    weights = [1] * 10
    elites = [.25, 30, 100, 5, .1, 300, 10, 5, 50, 500]
    decision = []
    learningRate = 0.1

    with open(filename, 'r') as csvFile:
        csvFil = csv.reader(csvFile)
        for row in csvFil:
            eliteSum = 0
            notEliteSum = 0
            for ind, indicator,eliteVal,weightVal in zip(range(10),row,elites,weights):
                indicator = float(indicator)
                if indicator > eliteVal:
                    eliteSum += weightVal
                else:
                    notEliteSum += weightVal
                if (int(row[10]) == 1 and indicator < eliteVal) or (int(row[10]) == 0 and indicator > eliteVal):
                    weights[ind] *= 1 - learningRate

            decision.append(1 if eliteSum > notEliteSum else 0)


    csvFile.close()
    return weights

print weightedMajorityAlgorithm("example.csv")
