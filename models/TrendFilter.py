"""L1-Trend Filter Model"""

from pyspark.mllib.regression import LassoModel,LassoWithSGD
from pyspark.sql.session import SparkSession


class Trend_Filter():
    def __init__(self,sc,x,y,k,reg_param,opt_params):

        self.data=None
        self.opt_params=opt_params


    def fit(self):


        model = LassoWithSGD.train(self.sc.parallelize(self.data), iterations=self.opt_params['iter'], initialWeights=None)


    def predict(self):
        pass


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext