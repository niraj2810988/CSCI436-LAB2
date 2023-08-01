from mrjob.job import MRJob

class BigramCountMRJob(MRJob):

    def mapper(self, _, line):
        
        words = line.strip().lower().split()

        
        for i in range(len(words) - 1):
            bigram = words[i] + ',' + words[i+1]
            yield bigram, 1

    def combiner(self, bigram, counts):
        
        yield bigram, sum(counts)

    def reducer(self, bigram, counts):
        
        total_count = sum(counts)

        
        yield bigram, total_count

if __name__ == '__main__':
    BigramCountMRJob.run()
