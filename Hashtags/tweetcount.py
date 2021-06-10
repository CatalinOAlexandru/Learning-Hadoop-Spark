from mrjob.job import MRJob
import re


WORD_REGEX = re.compile(r"\b\w+\b")

class tweetcount(MRJob):

    def mapper(self, _, line):
        fields = line.split(";")
        try:
            if(len(fields)==4):
                epoch_time = int(field[0])/1000
                day = time.strftime("%d", time.gmtime(epoch_time))
                yield(day, 1)
        except:
            pass


    def reducer(self, key, values):
        yield(day, sum(counts))

if __name__ == '__main__':
    tweetcount.run()
