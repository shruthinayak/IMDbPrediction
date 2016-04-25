import collections
import nltk.classify.util, nltk.metrics
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import movie_reviews
import csv
import pickle

def evaluate_classifier(featx):
	negids = movie_reviews.fileids('neg')
	posids = movie_reviews.fileids('pos')

	negfeats = [(featx(movie_reviews.words(fileids=[f])), 'neg') for f in negids]
	posfeats = [(featx(movie_reviews.words(fileids=[f])), 'pos') for f in posids]

	negcutoff = len(negfeats)*3/4
	poscutoff = len(posfeats)*3/4

	trainfeats = negfeats[:negcutoff] + posfeats[:poscutoff]
	testfeats = negfeats[negcutoff:] + posfeats[poscutoff:]
	global classifier
	classifier = NaiveBayesClassifier.train(trainfeats)
	refsets = collections.defaultdict(set)
	testsets = collections.defaultdict(set)
	
	for i, (feats, label) in enumerate(testfeats):
			refsets[label].add(i)
			observed = classifier.classify(feats)
			testsets[observed].add(i)
			

def word_feats(words):
	return dict([(word, True) for word in words])
	
evaluate_classifier(word_feats)

f = open('my_classifier.pickle', 'wb')
pickle.dump(classifier, f)
f.close()
