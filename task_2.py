from mrjob.job import MRJob
from mrjob.step import MRStep


class TempDataSet(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.get_min_temps, reducer =self.reducer_minimum)
        ]
    
    def get_min_temps(self, _ , line):
        wheather_station, _, obs_type, temp,_,_,_,_= line.split(",")
        if wheather_station in ["ITE00100554", "EZE00100082"] and obs_type=="TMIN":
            yield  wheather_station, float(temp)

    def reducer_minimum(self, key, values):
        yield key, min(values)/10
        


if __name__=="__main__":
    TempDataSet.run()