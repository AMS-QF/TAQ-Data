from sklearn.metrics import r2_score

def out_of_sample_R2(y_test, y_predict):
    """
    Compute Out-of-sample R^2
    """
    return r2_score(y_test, y_predict)
