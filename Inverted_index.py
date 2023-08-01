from mrjob.job import MRJob

class InvertedIndexMRJob(MRJob):

    def mapper(self, _, line):
       
        doc_id, content = line.strip().split(': ', 1)

        
        words = content.lower().split()
        for word in words:
            
            word = word.strip('.,?!-:;()"\'')
            if word:
                yield word, doc_id

    def reducer(self, word, doc_ids):
        
        unique_doc_ids = []

        
        for doc_id in doc_ids:
            if doc_id not in unique_doc_ids:
                unique_doc_ids.append(doc_id)

        
        unique_doc_ids.sort()

        yield word, unique_doc_ids

if __name__ == '__main__':
    InvertedIndexMRJob.run()
