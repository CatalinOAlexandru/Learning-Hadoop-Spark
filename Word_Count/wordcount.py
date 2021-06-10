"""Lab 1. Basic wordcount
"""
from mrjob.job import MRJob
import re

#this is a regular expression that finds all the words inside a String
WORD_REGEX = re.compile(r"\b\w+\b")
wordss = 0
#This line declares the class Lab1, that extends the MRJob format.
class Lab1(MRJob):
    wordss = 0
# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            ++wordss
            yield (word.lower(), 1)
            print(wordss)

#and the reducer method goes after this line
    def reducer(self, word, counts):
        count = sum(counts)
        if count > 10:
            yield(word, count)


#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    wordss = 0
    Lab1.run()
    print(wordss)

print(wordss)
