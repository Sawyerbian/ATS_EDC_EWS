import pandas as pd
import numpy as np
from glob import glob
import os
from pathlib import Path
from datetime import date
import json
import time
from config import *
from send_mail import send_report

def EDC_MONITOR():
    Customer_summary_Output_By_FailMonth = pd.read_excel("D:\ATS_EDC_EWS\Final_Output_folder\Customer_summary_Output_By_FailMonth.xlsx")
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Fail_Month.notna()]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.TNS_QTY.notna()]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Totol_Cost_Sum.notna()]
    Customer_summary_Output_By_FailMonth.loc[:,"year"] = Customer_summary_Output_By_FailMonth.loc[:,"Fail_Month"].apply(lambda x:int(str(x)[:4]))
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth[["Customer_Name","Totol_Cost_Sum","TNS","year"]].groupby(by=["Customer_Name","year"]).sum()
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"TNS"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"TNS"].apply(lambda x:np.abs(x))
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"Totol_Cost_Sum"]/ Customer_summary_Output_By_FailMonth_year_edc.loc[:,"TNS"]
    #Customer_summary_Output_By_FailMonth_year_edc.to_excel("Customer_summary_Output_By_FailMonth_year_edc.xlsx")
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"current_year"] = date.today().year
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"current_month"] = date.today().month
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_edc[Customer_summary_Output_By_FailMonth_year_edc.index.get_level_values("year") == Customer_summary_Output_By_FailMonth_year_edc.current_year]
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_edc[Customer_summary_Output_By_FailMonth_year_edc.edc > EDC]
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"].apply(lambda x:float(x * 100))
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"].apply(lambda x:np.round(x,2).astype(str) + ' %')
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_edc.reset_index().drop(columns="year")
    Customer_summary_Output_By_FailMonth_year_edc.to_excel("Customer_summary_Output_By_FailMonth_year_edc.xlsx")
    for json_data in json.loads(Customer_summary_Output_By_FailMonth_year_edc.to_json(orient='records')):
        Customer_Name = json_data["Customer_Name"]
        current_year = json_data["current_year"]
        current_month = json_data["current_month"]
        edc =json_data["edc"]
        send_report(Subject="EDC Warning",content_1=f"您好， {Customer_Name}客户,{current_year}年{current_month}月YTD EDC={edc}, 已超过公司质量目标，请调查原因及制定改善措施，谢谢。",to_all=False)
        time.sleep(10)


def SHORT_KM_MONITOR():
    Customer_summary_Output_By_FailMonth = pd.read_excel("D:\ATS_EDC_EWS\Final_Output_folder\Customer_summary_Output_By_FailMonth.xlsx")
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Fail_Month.notna()]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.TNS_QTY.notna()]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Totol_Cost_Sum.notna()]
    Customer_summary_Output_By_FailMonth.loc[:,"year"] = Customer_summary_Output_By_FailMonth.loc[:,"Fail_Month"].apply(lambda x:int(str(x)[:4]))
    Customer_summary_Output_By_FailMonth.loc[:,"month"] = Customer_summary_Output_By_FailMonth.loc[:,"Fail_Month"].apply(lambda x:int(str(x)[5:7]))
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth[["Customer_Name","Product_Name","Mil<1000 Qty","year","month"]].groupby(by=["Customer_Name","Product_Name","year","month"]).sum()
    Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"current_year"] = date.today().year
    Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"current_month"] = date.today().month
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.index.get_level_values("year") == Customer_summary_Output_By_FailMonth_year_complaint.current_year]
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"Mil<1000 Qty"] > KM_COMPLAINT]
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint.reset_index().sort_values("Customer_Name")
    Customer_summary_Output_By_FailMonth_year_complaint.to_excel("Customer_summary_Output_By_FailMonth_year_complaint.xlsx")
    for customer in Customer_summary_Output_By_FailMonth_year_complaint.Customer_Name.unique():
        customer_df = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.Customer_Name == customer]
        report_text = "\n"
        report_list= []
        for json_data in json.loads(customer_df.to_json(orient='records')):
            Customer_Name = json_data["Customer_Name"]
            Product_Name = json_data["Product_Name"]
            current_year = json_data["year"]
            current_month = json_data["month"]
            failure_quantity =json_data["Mil<1000 Qty"]
            report_list.append(f"{Customer_Name}客户 {Product_Name}产品{current_year}年{current_month}月 1000KM失效={failure_quantity}")
            report_tuple = tuple(report_list)
        report_text = report_text.join(report_tuple)
        send_report(Subject="1000KM Complaint Warning",content_1=f"您好:\n {report_text}, 已超过公司质量目标，请调查原因及制定改善措施，谢谢。",to_all=False)
        time.sleep(10)


if __name__ == "__main__":
    while True:
        today = date.today().day
        if today == 8:
            print("in")
            try:
                # EDC_MONITOR()
                SHORT_KM_MONITOR()
            except Exception as e:
                print(e)
        else:
            print(f"today is {today}")
        time.sleep(144000)