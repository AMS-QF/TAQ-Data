from spark_pipelines import generators
import unittest
import pandas as pd
import math

class SpeedCostTest(unittest.TestCase):
    def getTestDataFrame(self):
        testDF = pd.DataFrame(
            {
                'RID':[244031,0,1,2,3,4,244032,244033,5,244034,244035,6,7],
                'Symbol':['AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL','AAPL'],
                'Trade_Price':[200,300,350,270,500,430,600,550,700,650,620,750,540],
                'Trade_Volume':[0,0,0,0,0,0,0,0,0,0,0,0,0],
                'Participant_Timestamp_f':[0.4,0.1,0.4,0.3,0.25,0.2,0.5,15700000000,0.6,15700000000,15700000000,15700000000,15700000000],
                'Is_Quote':[True,False,False,False,True,False,True,True,False,True,True,True,True]
            }
        )
        return testDF
    
    def test_GetPreviousTimeStamp(self):
        testDF = self.getTestDataFrame()
        actual = generators.getPreviousTimeStamp(testDF,0.6)
        expected = 0.4
        self.assertEqual(actual, expected)
        actual2 = generators.getPreviousTimeStamp(testDF,0.3)
        expected2 = 0.2
        self.assertEqual(actual2, expected2)
    
    def test_getLookBackIntervalForCalendar_t(self):
        testDF = self.getTestDataFrame()
        actual = generators.getLookBackIntervalForCalendar_t(testDF,0.6,0.1,0.3)['Participant_Timestamp_f'].tolist()
        self.assertEqual(True, 0.4 in actual)
        self.assertEqual(True, 0.6 not in actual)
        self.assertEqual(True, 0.5 in actual)
        self.assertEqual(True, 0.3 not in actual)

    def test_getAutoCovariance(self):
        testDF = self.getTestDataFrame()
        actual = generators.getAutoCovariance(testDF,0.1,0.3,'calendar')
        expected = [float("nan"), float("nan"), -0.16753204220586432, float("nan"), float("nan"), float("nan"), 
                    float("nan"), float("nan"), -0.12076697326167687, float("nan"), float("nan"), float("nan"), float("nan")]
        #0.4 case should be NaN or -0.16753204220586432
        # use log or ln (right now ln)
        for i in range(len(actual)):
            if(math.isnan(actual[i]) and math.isnan(expected[i])):
                continue
            self.assertEqual(actual[i], expected[i])
    

if __name__ == '__main__':
    unittest.main()
