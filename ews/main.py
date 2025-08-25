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
from itertools import product
from datetime import datetime, timedelta
import shutil
import glob
from smbclient import register_session, scandir, open_file
source_xlsx_dir_path = r"\\bosch.com\dfsRB\DfsCN\loc\WX4\Dept\TER\10_QMM\13 QMC\14 Reporting\00 Monthly quality reports\PowerBi\Customer 2023\Able to clean customers\Final_Output_folder\*"
from dotenv import load_dotenv
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)
xlsx_dir_path = os.getenv("xlsx_dir_path", "./xlsx_files")

# def sychronize_data():
#     os.makedirs(os.path.dirname(xlsx_dir_path), exist_ok=True)
#     print(f"{xlsx_dir_path} created.")
#     logger.info(f"{xlsx_dir_path} created.")
#     source_files_path = glob.glob(source_xlsx_dir_path)
#     for path in source_files_path:
#         print(f"find {path}")
#         logger.info(f"find {path}")
#     for source_path in glob.glob(source_xlsx_dir_path):
#         filename = os.path.basename(source_path)
#         dest_path = os.path.join(xlsx_dir_path, filename)
#         shutil.copy2(source_path, dest_path)
#         print(f"Copied: {filename}")
#         logger.info(f"Copied: {filename}")

#     print("Matching files copied!")






from smbclient import register_session, path as smb_path, ClientConfig
import os
import logging

# Configure detailed logging
from smbclient import register_session, ClientConfig, listdir, open_file, mkdir
import os
import shutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def synchronize_data():
    try:
        # Configure credentials globally
        ClientConfig(
            username=os.getenv("NT_USER"),
            password=os.getenv("NT_PASSWORD"),
            domain="bosch.com"
        )
        
        # Server configurations
        servers = [
            {
                "server": "SGP0DFS403.APAC.BOSCH.COM",
                "share": "DfsCN",
                "path": r"\loc\WX4\Dept\TER\10_QMM\13 QMC\14 Reporting\00 Monthly quality reports\PowerBi\Customer 2023\Able to clean customers\Final_Output_folder"
            },
            {
                "server": "wx40fs02.apac.bosch.com",
                "share": "Wx4_Dept_MOE$",
                "path": r"\TER\10_QMM\13 QMC\14 Reporting\00 Monthly quality reports\PowerBi\Customer 2023\Able to clean customers\Final_Output_folder"
            }
        ]
        
        # Try each server configuration
        for config in servers:
            try:
                full_path = rf"\\{config['server']}\{config['share']}{config['path']}"
                print(f"Attempting to access: {full_path}")
                
                # Check if path exists
                try:
                    test_file = listdir(full_path)[0]  # Try listing first file
                    print(f"Found {len(listdir(full_path))} files in directory")
                    break
                except Exception as e:
                    print(f"Access failed: {str(e)}")
                    continue
            except Exception as e:
                print(f"Configuration error: {str(e)}")
                continue
        else:
            raise ValueError("Could not access any configured server paths")
        
        # Ensure local directory exists
        os.makedirs(xlsx_dir_path, exist_ok=True)
        
        # Copy files
        for filename in listdir(full_path):
            if filename.lower().endswith('.xlsx'):
                remote_path = rf"{full_path}\{filename}"
                local_path = os.path.join(xlsx_dir_path, filename)
                
                try:
                    with open_file(remote_path, mode='rb') as remote_file:
                        with open(local_path, 'wb') as local_file:
                            shutil.copyfileobj(remote_file, local_file)
                    print(f"✓ Copied {filename}")
                except Exception as e:
                    print(f"✗ Failed to copy {filename}: {str(e)}")
                    
    except Exception as e:
        print(f"❗ Critical error: {str(e)}")
        raise



def EDC_MONITOR():
    def filter_month(current_month : int):
        if date.today().day > 20:
            return current_month - 1
        else:
            return current_month - 2
    Customer_summary_Output_By_FailMonth = pd.read_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth.xlsx"))

    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Fail_Month.notna()]
    Customer_summary_Output_By_FailMonth.loc[:,"Totol_Cost_Sum"] = Customer_summary_Output_By_FailMonth.loc[:,"Totol_Cost_Sum"].fillna(0)
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.loc[:,"Material"].apply(lambda x:len(str(x))==12)]  # EXCLUDE ["非RBCW产品",NAN]
    # Customer_summary_Output_By_FailMonth.loc[:,"year"] = Customer_summary_Output_By_FailMonth.loc[:,"Fail_Month"].apply(lambda x:int(str(x)[:4]))
    Customer_summary_Output_By_FailMonth['year'] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Fail_Month'], errors='coerce').dt.year
    Customer_summary_Output_By_FailMonth['month'] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Fail_Month'], errors='coerce').dt.month
    Customer_summary_Output_By_FailMonth.to_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth_detail.xlsx"))
    row_df = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.year == date.today().year]
    row_df = row_df[Customer_summary_Output_By_FailMonth.month == filter_month(date.today().month)]
    Customer_summary_Output_By_FailYear = Customer_summary_Output_By_FailMonth.dropna(subset="TNS_Year")
    Customer_summary_Output_By_FailYear = Customer_summary_Output_By_FailYear.drop_duplicates(subset=["Customer_Name", "Material","year"])
    Customer_summary_Output_By_FailYear = Customer_summary_Output_By_FailYear.loc[:,["Customer_Name", "year", "TNS_Year"]]
    Customer_summary_Output_By_FailYear = Customer_summary_Output_By_FailYear.groupby(by=["Customer_Name","year"]).sum()
    Customer_summary_Output_By_FailMonth_year_cost = Customer_summary_Output_By_FailMonth[["Customer_Name","Totol_Cost_Sum","year"]].groupby(by=["Customer_Name","year"]).sum()
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_cost.merge(Customer_summary_Output_By_FailYear, left_index=True,right_index=True,how="right")
    # Customer_summary_Output_By_FailMonth_year_edc.loc[:,"TNS"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"TNS"].apply(lambda x:np.abs(x))
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"Totol_Cost_Sum"]/ Customer_summary_Output_By_FailMonth_year_edc.loc[:,"TNS_Year"]
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"current_year"] = date.today().year
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"current_month"] = date.today().month


    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"current_month"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"current_month"].apply(lambda x:filter_month(x))
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_edc[Customer_summary_Output_By_FailMonth_year_edc.index.get_level_values("year") == Customer_summary_Output_By_FailMonth_year_edc.current_year]
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_edc[Customer_summary_Output_By_FailMonth_year_edc.edc > EDC]
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"].apply(lambda x:float(x * 100))
    Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"] = Customer_summary_Output_By_FailMonth_year_edc.loc[:,"edc"].apply(lambda x:np.round(x,2).astype(str) + ' %')

    Customer_summary_Output_By_FailMonth_year_edc.to_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth_year_edc.xlsx"))
    Customer_summary_Output_By_FailMonth_year_edc = Customer_summary_Output_By_FailMonth_year_edc.reset_index()

    for customer in Customer_summary_Output_By_FailMonth_year_edc.Customer_Name.unique():
        customer_df = Customer_summary_Output_By_FailMonth_year_edc[Customer_summary_Output_By_FailMonth_year_edc.Customer_Name == customer]
        report_text = "\n"
        report_list= []
        for json_data in json.loads(customer_df.to_json(orient='records')):
            Customer_Name = json_data["Customer_Name"]
            # Product_Name = json_data["Product_Name"]
            current_year = json_data["year"]
            current_month = json_data["current_month"]
            break
        report_tuple = tuple(report_list)
        report_text = report_text.join(report_tuple)
        df2 = row_df[row_df.Customer_Name == customer]
        send_report(Subject="EDC Warning",content_1=f"您好， {Customer_Name}客户,{current_year}年{current_month}月YTD, 已超过公司质量目标，请调查原因及制定改善措施，谢谢。",df=customer_df, df2=df2, to_all=False, to_group=customer)
        time.sleep(10)
    return True

def SHORT_KM_MONITOR():
    def filter_month(current_month : int):
        if date.today().day > 20:
            return current_month -1
        else:
            return current_month - 2
    Customer_summary_Output_By_FailMonth = pd.read_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output.xlsx"))
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Mileage <= 1000]
    Customer_summary_Output_By_FailMonth['year'] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Statistic month'], errors='coerce').dt.year
    Customer_summary_Output_By_FailMonth['month'] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Statistic month'], errors='coerce').dt.month
    row_df = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.year == date.today().year]
    row_df = row_df[Customer_summary_Output_By_FailMonth.month == filter_month(date.today().month)]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[["Customer", "Statistic month","Mileage"]].groupby(["Customer", "Statistic month"]).count()
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth.rename(columns={"Mileage":"Mil<1000 Qty"})
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth.reset_index()

    Customer_summary_Output_By_FailMonth.loc[:,"year"] = Customer_summary_Output_By_FailMonth.loc[:,"Statistic month"].apply(lambda x:int(str(x)[:4]))
    Customer_summary_Output_By_FailMonth['year'] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Statistic month'], errors='coerce').dt.year
    Customer_summary_Output_By_FailMonth['month'] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Statistic month'], errors='coerce').dt.month

    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth
    Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"current_year"] = date.today().year
    Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"current_month"] = date.today().month


    Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"current_month"] = Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"current_month"].apply(lambda x:filter_month(x))
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.year == Customer_summary_Output_By_FailMonth_year_complaint.current_year]
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.month == Customer_summary_Output_By_FailMonth_year_complaint.current_month]
    Customer_summary_Output_By_FailMonth_year_complaint.to_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth_year_complaint.xlsx"))

    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.loc[:,"Mil<1000 Qty"] > KM_COMPLAINT]
    Customer_summary_Output_By_FailMonth_year_complaint = Customer_summary_Output_By_FailMonth_year_complaint.reset_index(drop=True).sort_values("Customer")


    for customer in Customer_summary_Output_By_FailMonth_year_complaint.Customer.unique():
        customer_df = Customer_summary_Output_By_FailMonth_year_complaint[Customer_summary_Output_By_FailMonth_year_complaint.Customer == customer]
        report_text = "\n"
        report_list= []
        for json_data in json.loads(customer_df.to_json(orient='records')):
            Customer_Name = json_data["Customer"]
            # Product_Name = json_data["Product_Name"]
            current_year = json_data["year"]
            current_month = json_data["month"]
            failure_quantity =json_data["Mil<1000 Qty"]
            break
        report_list.append(f"{Customer_Name}客户 {current_year}年{current_month}月 1000KM失效={failure_quantity}")
        report_tuple = tuple(report_list)
        report_text = report_text.join(report_tuple)
        df2 = row_df[row_df.Customer == customer]
        new_qty = len(df2)
        send_report(Subject="1000KM Complaint Warning",content_1=f"您好:\n {report_text}, {current_month}月新增<1000Km失效{new_qty}件,请调查原因并提供改进措施，谢谢。", df=customer_df,df2=df2,to_all=False, to_group=customer)
        time.sleep(10)
    return True

def FAIL_QTY_MONITOR():
    Customer_summary_Output_By_FailMonth = pd.read_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth.xlsx"))

    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Fail_Month.notna()]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.TNS_QTY.notna()]
    Customer_summary_Output_By_FailMonth = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.Totol_Cost_Sum.notna()]
    Customer_summary_Output_By_FailMonth.loc[:,"year"] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Fail_Month'], errors='coerce').dt.year
    Customer_summary_Output_By_FailMonth.loc[:,"month"] = pd.to_datetime(Customer_summary_Output_By_FailMonth['Fail_Month'], errors='coerce').dt.month
    row_df = Customer_summary_Output_By_FailMonth[Customer_summary_Output_By_FailMonth.year == date.today().year]
    row_df = row_df[row_df.month == date.today().month]
    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth[["Customer_Name","Product_Name","Fail QYT","year","month"]].groupby(by=["Customer_Name","Product_Name","year","month"]).sum()
    Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_year"] = date.today().year
    Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_month"] = date.today().month
    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth_exchange[(Customer_summary_Output_By_FailMonth_exchange.index.get_level_values("year") == Customer_summary_Output_By_FailMonth_exchange.current_year) | (Customer_summary_Output_By_FailMonth_exchange.index.get_level_values("year") == Customer_summary_Output_By_FailMonth_exchange.current_year - 1)]

    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth_exchange.reset_index().sort_values("Customer_Name")

    Customer_Name = list(Customer_summary_Output_By_FailMonth_exchange.Customer_Name.unique())
    Product_Name = list(Customer_summary_Output_By_FailMonth_exchange.Product_Name.unique())
    year = list(Customer_summary_Output_By_FailMonth_exchange.year.unique())
    month = [x for x in range(1,13)]
    master_df = pd.DataFrame(list(product(Customer_Name, Product_Name, year, month)), columns=["Customer_Name", "Product_Name", "year", "month"])
    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth_exchange.merge(master_df,on=["Customer_Name", "Product_Name", "year", "month"],how="right")
    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth_exchange.sort_values(["Customer_Name","Product_Name","year","month"])
    Customer_summary_Output_By_FailMonth_exchange.loc[:,"Fail QYT"] = Customer_summary_Output_By_FailMonth_exchange.loc[:,"Fail QYT"].fillna(0)
    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth_exchange.ffill()
    Customer_summary_Output_By_FailMonth_exchange = Customer_summary_Output_By_FailMonth_exchange.bfill()
    Customer_summary_Output_By_FailMonth_exchange.loc[:,"incremental"] = Customer_summary_Output_By_FailMonth_exchange.groupby(["Customer_Name","Product_Name"])["Fail QYT"].diff()


    def filter_month(current_month : int):
        if date.today().day > 20:
            return [current_month - 2, current_month-1]
        else:
            return [current_month - 3, current_month - 2]

    Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_month"] = Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_month"].apply(lambda x:filter_month(x))
    Customer_summary_Output_By_FailMonth_exchange_2month = Customer_summary_Output_By_FailMonth_exchange[Customer_summary_Output_By_FailMonth_exchange.month.isin(Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_month"].drop_duplicates().values[0])]
    Customer_summary_Output_By_FailMonth_exchange_2month = Customer_summary_Output_By_FailMonth_exchange_2month.groupby(["Customer_Name","year","Product_Name"]).min()[["incremental"]]
    Customer_summary_Output_By_FailMonth_exchange_2month = Customer_summary_Output_By_FailMonth_exchange_2month[Customer_summary_Output_By_FailMonth_exchange_2month.index.get_level_values("year") == date.today().year]
    Customer_summary_Output_By_FailMonth_exchange_2month.loc[:,"month"] = max(Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_month"].drop_duplicates().values[0])
    Customer_summary_Output_By_FailMonth_exchange_2month.to_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth_exchange_2month.xlsx"))
    Customer_summary_Output_By_FailMonth_exchange_2month = Customer_summary_Output_By_FailMonth_exchange_2month[Customer_summary_Output_By_FailMonth_exchange_2month.incremental > 0]
    Customer_summary_Output_By_FailMonth_exchange_2month = Customer_summary_Output_By_FailMonth_exchange_2month.reset_index().sort_values("Customer_Name")

    Customer_summary_Output_By_FailMonth_exchange_12month = Customer_summary_Output_By_FailMonth_exchange
    Customer_summary_Output_By_FailMonth_exchange_12month.loc[:,"current_month"] = max(Customer_summary_Output_By_FailMonth_exchange.loc[:,"current_month"].drop_duplicates().values[0])
    Customer_summary_Output_By_FailMonth_exchange_12month.to_excel(os.path.join(xlsx_dir_path,"Customer_summary_Output_By_FailMonth_exchange_12month.xlsx"))
    df = Customer_summary_Output_By_FailMonth_exchange_12month
    # Assume df is your DataFrame
    df['date'] = pd.to_datetime(df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str) + '-01')
    df['current_date'] = pd.to_datetime(df['current_year'].astype(int).astype(str) + '-' + df['current_month'].astype(int).astype(str) + '-01')

    # Filter rows where 'date' is within 12 months before 'current_date'
    mask = df['date'] > df['current_date'] - timedelta(days=365)
    df_12m = df[mask].copy()

    # Group by keys and compute average of previous 12 months
    avg_df = (
        df_12m
        .groupby(['Customer_Name', 'Product_Name', 'current_year', 'current_month'])['Fail QYT']
        .mean()
        .reset_index()
        .rename(columns={'Fail QYT': 'last_12_month'})
    )

    # Keep only rows that match current_year and current_month
    df_current = df[
        (df['year'] == df['current_year']) & 
        (df['month'] == df['current_month'])
    ].copy()

    # Merge the calculated average into current-month rows
    final_df = pd.merge(df_current, avg_df, on=['Customer_Name', 'Product_Name', 'current_year', 'current_month'], how='left')

    # Drop helper columns if not needed
    final_df.drop(columns=['date', 'current_date'], errors='ignore', inplace=True)

    # Final result
    final_df
    # final_df.to_excel("C:\disk\AI\ATS_EDC_EWS\data_for_power_bi\Customer_summary_Output_By_FailMonth_exchange_12month.xlsx")
    Customer_summary_Output_By_FailMonth_exchange_12month = final_df[final_df.last_12_month < final_df.loc[:,"Fail QYT"]]

    for customer in Customer_summary_Output_By_FailMonth_exchange_2month.Customer_Name.unique():
        customer_df = Customer_summary_Output_By_FailMonth_exchange_2month[Customer_summary_Output_By_FailMonth_exchange_2month.Customer_Name == customer].reset_index(drop=True)
        report_text = "\n"
        report_list= []
        for json_data in json.loads(customer_df.to_json(orient='records')):
            Customer_Name = json_data["Customer_Name"]
            Product_Name = json_data["Product_Name"]
            current_year = json_data["year"]
            current_month = json_data["month"]
            break
        report_list.append(f"{Customer_Name}客户 {current_year}年{current_month}月 连续两个月换件数量增加")
        report_tuple = tuple(report_list)
        report_text = report_text.join(report_tuple)
        df2 = row_df[row_df.Customer_name == customer]
        send_report(Subject="换件数量监控",content_1=f"您好:\n {report_text}, 已超过公司质量目标，请调查原因及制定改善措施，谢谢。", df=customer_df,df2=df2,to_all=False, to_group=customer)
        time.sleep(10)

    for customer in Customer_summary_Output_By_FailMonth_exchange_12month.Customer_Name.unique():
        customer_df = Customer_summary_Output_By_FailMonth_exchange_12month[Customer_summary_Output_By_FailMonth_exchange_12month.Customer_Name == customer]
        report_text = "\n"
        report_list= []
        for json_data in json.loads(customer_df.to_json(orient='records')):
            Customer_Name = json_data["Customer_Name"]
            Product_Name = json_data["Product_Name"]
            current_year = json_data["year"]
            current_month = json_data["month"]
            break
        report_list.append(f"{Customer_Name}客户 {Product_Name}产品 {current_year}年{current_month}月 换件数量超过过去12月平均值")
        report_tuple = tuple(report_list)
        report_text = report_text.join(report_tuple)
        df2 = row_df[row_df.Customer_name == customer]
        send_report(Subject="换件数量监控",content_1=f"您好:\n {report_text}, 已超过公司质量目标，请调查原因及制定改善措施，谢谢。",df=customer_df, df2=df2,to_all=False, to_group=customer)
    return True

import sqlite3
import time
from datetime import datetime, date

DB_NAME = "task_log.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS task_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT NOT NULL,
            exec_date TEXT NOT NULL,
            year_month TEXT NOT NULL,
            success INTEGER NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def has_successful_run_this_month(task_name):
    now = datetime.now()
    year_month = now.strftime('%Y-%m')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        SELECT COUNT(*) FROM task_log
        WHERE task_name = ? AND year_month = ? AND success = 1
    ''', (task_name, year_month))
    result = c.fetchone()[0]
    conn.close()
    return result > 0

def log_task(task_name, success):
    now = datetime.now()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        INSERT INTO task_log (task_name, exec_date, year_month, success)
        VALUES (?, ?, ?, ?)
    ''', (task_name, now.isoformat(), now.strftime('%Y-%m'), int(success)))
    conn.commit()
    conn.close()

import logging



if __name__ == "__main__":
    init_db()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # logger.info("This is an info log")
    # logger.error("Something went wrong")

    while True:
        try:
            synchronize_data()
        except Exception as e:
            logger.info("sychronize data")
            logger.error(e)       

        today = date.today().day
        if today >= 23:
            if not has_successful_run_this_month("EDC_MONITOR"):
                print("EDC_MONITOR processing...")
                try:
                    EDC_MONITOR()
                    log_task("EDC_MONITOR", True)
                except Exception as e:
                    print("EDC Error:", e)
                    logger.info("EDC")
                    logger.error(e)
                    log_task("EDC_MONITOR", False)

            # SHORT_KM
            if not has_successful_run_this_month("SHORT_KM_MONITOR"):
                try:
                    print("SHORT_KM processing...")
                    SHORT_KM_MONITOR()
                    log_task("SHORT_KM_MONITOR", True)
                except Exception as e:
                    print("SHORT_KM Error:", e)
                    logger.info("SHORT_KM")
                    logger.error(e)
                    log_task("SHORT_KM_MONITOR", False)

            # FAIL_QTY
            if not has_successful_run_this_month("FAIL_QTY_MONITOR"):
                try:
                    print("FAIL_QTY_MONITOR processing...")
                    FAIL_QTY_MONITOR()
                    log_task("FAIL_QTY_MONITOR", True)
                except Exception as e:
                    print("FAIL_QTY Error:", e)
                    logger.info("FAIL_QTY")
                    logger.error(e)
                    log_task("FAIL_QTY_MONITOR", False)
        else:
            print(f"Today is {today}. Waiting for day >= 23.")

        # Sleep for 2 days
        logger.info("Sleeping")
        time.sleep(1 * 24 * 3600)
