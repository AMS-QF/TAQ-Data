import numpy as np


def out_of_sample_R2(y_true, y_predict):
  
    SS_res = np.sum((y_true - y_predict) ** 2)

    mean_y_true = np.mean(y_true)
    SS_tot = np.sum((y_true - mean_y_true) ** 2)

    return 1 - (SS_res / SS_tot)
