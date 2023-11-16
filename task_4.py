from mrjob.job import MRJob
from mrjob.step import MRStep


class CustomerDataSet(MRJob):
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
        id,_ ,amount= line.split(",")
        yield id, float(amount)

    def combine_word_counts(self, id, amount):
        yield id, sum(amount)

    def reducer_sum_word_counts(self, key, values):
        yield None, ( sum(values), key)

    def reduce_sort_counts(self, _, total_spendings):
        for amount, key in sorted(total_spendings, reverse=True):
            yield (key, amount)


if __name__=="__main__":
    CustomerDataSet.run()