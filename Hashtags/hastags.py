from mrjob.job import MRJob
import re


class hastags(MRJob):

    def mapper(self, _, line):
        fields = line.split(";")
        try:
            if(len(fields)==4):
                length = len(fields[2])
                hashes = fields[2].count("#")
                yield ("length", length)
                yield ("hashtags", hashes)
        except:
            pass


    def reducer(self, key, values):
        try:
            sum = 0
            n = 0
            for value in values:
                sum += value
                n += 1
            avg = sum/n
            yield(key,avg)
        except:
            pass

if __name__ == '__main__':
    hastags.run()
