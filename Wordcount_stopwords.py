from mrjob.job import MRJob


STOPWORDS = set(['the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'])

class NonStopWordCountMRJob(MRJob):

    def configure_args(self):
        
        super(NonStopWordCountMRJob, self).configure_args()
        self.add_passthru_arg('--stopwords', default=','.join(STOPWORDS),
                              help='Comma-separated list of stopwords')

    def mapper_init(self):
        
        self.stopwords = set(self.options.stopwords.split(','))

    def mapper(self, _, line):
        
        words = line.strip().lower().split()

        
        for word in words:
            
            word = word.strip('.,?!-:;()"\'')
            if word and word not in self.stopwords:
                yield word, 1

    def combiner(self, word, counts):
        
        yield word, sum(counts)

    def reducer(self, word, counts):
        
        total_count = sum(counts)

        
        yield word, total_count

if __name__ == '__main__':
    NonStopWordCountMRJob.run()
