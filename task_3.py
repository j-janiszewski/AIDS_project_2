from mrjob.job import MRJob
from mrjob.step import MRStep


class BookDataSet(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_words, combiner=self.combine_word_counts,
                reducer=self.reducer_sum_word_counts
            ),
            MRStep(
                reducer=self.reduce_sort_counts
            )
        ]

    def mapper_extract_words(self, _, line):
        for word in line.split(" "):
            yield word.lower(), 1

    def combine_word_counts(self, word, counts):
        yield word, sum(counts)

    def reducer_sum_word_counts(self, key, values):
        yield None, (sum(values), key)

    def reduce_sort_counts(self, _, word_counts):
        for count, key in sorted(word_counts, reverse=True):
            yield (int(count), key)


if __name__=="__main__":
    BookDataSet.run()