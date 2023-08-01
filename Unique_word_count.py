from mrjob.job import MRJob

class UniqueWordCountMRJob(MRJob):

    def mapper(self, _, line):
       
        words = line.strip().lower().split()

        
        unique_words_set = set()

        
        for word in words:
            
            word = word.strip('.,?!-:;()"\'')
            if word and word not in unique_words_set:
                unique_words_set.add(word)
                yield word, 1

    def combiner(self, word, counts):
        
        yield word, sum(counts)

    def reducer(self, word, counts):
        
        total_count = sum(counts)

        
        yield word, total_count

if __name__ == '__main__':
    UniqueWordCountMRJob.run()

