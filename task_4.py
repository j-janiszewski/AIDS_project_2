from mrjob.job import MRJob
from mrjob.step import MRStep


class CustomerDataSet(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_words ,
                reducer=self.reducer_sum_spendings_counts
            ),
            MRStep(
                reducer=self.reduce_sort_amounts
            )
        ]

    def mapper_extract_words(self, _, line):
        id,_ ,amount= line.split(",")
        yield id, float(amount)


    def reducer_sum_spendings_counts(self, key, values):
        yield None, ( sum(values), key)

    def reduce_sort_amounts(self, _, total_spendings):
        for amount, key in sorted(total_spendings, reverse=True):
            yield (key, amount)


if __name__=="__main__":
    CustomerDataSet.run()