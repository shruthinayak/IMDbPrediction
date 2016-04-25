import re
#import score
import looper
import os
import sys
execfile("/home/shruthi/Softwares/spark-1.5.1-bin-hadoop2.6/python/final_score.py")

#setClassPath()

from pyspark import SparkContext
sc = SparkContext(appName="Rating")

#no idea how to run with pyspark
#list of movie names
movieList = sc.textFile("/home/shruthi/AllFiles/Sem3/BD/movielist.txt")
#convert to a broadcast variable
broadMovies = sc.broadcast(movieList.collect())
#comments from movie subreddits
comments = sc.textFile("/home/shruthi/AllFiles/Sem3/BD/comments.csv")
#score.init()
#<comment, entire row>
keyComments = comments.map(lambda part : (re.findall('"([^"]*)"',part),part)).filter(lambda x : len(x[0])>0).map(lambda y : (y[0][0],y[1]))


#apply the function to each comment and filter which have movies in them. Output is <moviename, entire row>	
comm = keyComments.map(lambda x : (looper.checkEach(x[0], broadMovies.value),x[1])).filter(lambda y : len(y[0])>0)

# group movies by movie name. <moviename, List[row1, row2, ...]>
#groupedMovies = comm.groupByKey().map(lambda x : (x[0],list(x[1])))	
nlpMovies = comm.map(lambda x : (x[0], score(x[0],x[1]))).collect()
for i in nlpMovies:
	print i

