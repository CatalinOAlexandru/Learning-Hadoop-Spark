from mrjob.job import MRJob
import re

WORD_REGEX = re.compile(r"\b\w+\b")

class quiz2(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(';')
            if len(fields) == 4 :
                bin = 5*math.floor(int(len(fields[2]))/5)
                yield (bin, 1)
        except:
            pass
            #do nothing
    def reducer(self, bin, counts):
        yield (bin, sum(counts))

if __name__ == '__main__':
    quiz2.run()
