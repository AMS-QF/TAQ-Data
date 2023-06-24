#define a class that generate predictor variables according to page 12 of paper in case of concadinating them into pipeline
# !Remark: it does not generate a new line of feature to the dataframe since it may not always valid for different parameters sets.
class Volume_and_Duration(BaseEstimator, TransformerMixin):
    
    def __init__(self, X, T, delta1, delta2, M):
        self.X = X
        self.T = T
        self.delta1 = delta1
        self.delta2 = delta2
        self.M = M
        
    
    def fit(self, X, y=None):
        return self

    
    def Breadth(self):
        return backwards(X, T, delta1, delta2, M)['Trade_Price'].count()
    
    def Inmediacy(self):
        #it can be used on calender mode 'transaction' and 'volume', but I doubt whether it has practical meaning
        return len(backwards(X, T, delta1, delta2, M)['Participant_Timestamp_f'].value_counts())/ Breadth(self, X, T, delta1, delta2, M)
        
    
    def VolumeAll(self):
        return backwards(X, T, delta1, delta2, M)['Trade_Volume'].sum()
    
    def VolumeAvg(self):
        return VolumeAll(self, X, T, delta1, delta2, M)/ Breadth(self, X, T, delta1, delta2, M)
    
    def VolumeMax(self):
        return backwards(X, T, delta1, delta2, M)['Trade_Volume'].max()