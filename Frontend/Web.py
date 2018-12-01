import datetime
import boto3
import json

from flask import Flask, jsonify, abort, request, make_response, url_for
from flask import render_template, redirect

AWS_KEY=""
AWS_SECRET=""
REGION=""
BUCKET=""


app = Flask(__name__)
username = 'Need to input a username'
truth = 'NOT'
averageusefulreview = 0
usefulreviews = 0
reviews = 0
fans = 0
averagreviewlength = 0
consistency = 0
compliments = 0
votes = 0
time = ''

#WeightValues = [0.729, 0.729, 0.6561, 0.81, 0.254186582833, 0.729, 0.282429536481, 0.22876792455, 0.058149737003]
EliteValues = [102, 4, 100, 10, 40, 73.14, 1, 0.0018, 1]
WeightValues = [0,0,0,0,0,0,0,0,0]


@app.route('/', methods=['GET'])
def tweet_home():
    global WeightValues
    global EliteValues

    WeightValues = []
    fileExists = 1
    num = 1
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

    filename = 'elite2.csv'
    file = s3.get_object(Bucket=BUCKET,Key=filename)

    data = file['Body'].read()
    # print(str(data))
    splitdata = data.split(b'(')[-1]
    splitdata = splitdata[:len(splitdata)-3]
    weights = splitdata.split(b',')
    for weight in weights:
        weightNum = weight[1:]
        weightNum = float(weightNum)
        WeightValues.append(weightNum)
    print(str(WeightValues))

    return render_template('home.html')

@app.route('/testData', methods=['GET', 'POST'])
def get_data():
    global username
    global truth

    global WeightValues
    global EliteValues

    global averageusefulreview
    global usefulreviews
    global reviews
    global fans
    global averagreviewlength
    global consistency
    global compliments
    global votes
    global time
    if request.method == 'POST':
        username = str(request.form['name'])
        averagreviewlength = int(request.form['reviewlength']) 
        consistency = int(request.form['consistency']) 
        usefulreviews = int(request.form['usefulreviews']) 
        reviews = int(request.form['reviews'])
        fans = int(request.form['fans'])
        compliments = int(request.form['compliments'])
        votes = int(request.form['votes'])
        time = str(request.form['time'])
        newTime = ((datetime.datetime.now() - datetime.datetime.strptime(time,"%Y-%m-%d")).days) / 30
        if reviews == 0:
            averageusefulreview = 0
        else:
            averageusefulreview = float(usefulreviews) / float(reviews)
        #######
        #######
        indictatorValues = [averageusefulreview, usefulreviews, reviews, fans, averagreviewlength, consistency, compliments, votes, newTime]
        eliteSum = 0
        notEliteSum = 0
        for i in range(0,len(indictatorValues)):
            if float(indictatorValues[i]) > float(EliteValues[i]):
                eliteSum += WeightValues[i]
            else:
                notEliteSum += WeightValues[i]

        print("Elite sum: " + str(eliteSum) + " nonelite sum: " + str(notEliteSum))
        if eliteSum > notEliteSum:
            truth = 'YES'
        else:
            truth = 'NOT'
        #######
        #######
        return redirect('/showData')
    else:
        return render_template('add.html')

@app.route('/showData', methods=['GET'])
def show_data():
    global username
    global truth
    if truth == 'YES':
        return render_template('dataYes.html', username = username)
    elif truth == 'NOT':
        return render_template('dataNo.html', username = username)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
