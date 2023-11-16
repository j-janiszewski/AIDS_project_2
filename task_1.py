from mrjob.job import MRJob
from mrjob.step import MRStep


class FriendsDataSet(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.get_ages_and_friends, reducer =self.reducer_avg_friends_num)
        ]
    
    def get_ages_and_friends(self, _ , line):
        id, name, age, num_of_friends = line.split(",")
        yield (age, int(num_of_friends))

    def reducer_avg_friends_num(self, key, values):
        values = [x for x in values]
        yield key, sum(values)/ len(values)
        


if __name__=="__main__":
    FriendsDataSet.run()