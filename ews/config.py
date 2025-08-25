EDC = 0.005
KM_COMPLAINT = 2

import pandas as pd

def mailList():
    mailList = pd.read_excel("./EWSmailmatrix.xlsx").set_index("Customer name").fillna("None")
    return mailList.T.to_dict(orient="records")
