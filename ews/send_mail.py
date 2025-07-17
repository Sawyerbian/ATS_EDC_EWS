import smtplib
import email.utils
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import os
from dotenv import load_dotenv
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)
NT_PASSWORD = os.getenv("NT_PASSWORD")


# Prompt the user for connection info
bianhaizhong = "haizhong.bian@cn.bosch.com"
xupeng = "Peng.XU@cn.bosch.com"
renguochun = "Guochun.REN@cn.bosch.com"
zhangguangyao = "Guangyao.ZHANG@cn.bosch.com"
servername = 'rb-owa.apac.bosch.com'
#servername = raw_input('Mail server name: ')
username = 'bih1wx' 
#username = raw_input('User name: ')
#password = getpass.getpass("%s's password: " % username)
# password = NT_PASSWORD
# Create the message

def df_to_html_clean(df):
    # Convert DataFrame to HTML without index
    return df.to_html(index=False, classes="styled-table", border=0, escape=False)

def send_report(Subject=None, content_1= "", content_2="", df=pd.DataFrame(),df2=pd.DataFrame(),to_all= False):
    msg = MIMEMultipart("alternative")
    # msg.attach(MIMEText(f'Dear : Mr./Ms. \n {content_1}.\n {content_2}.\n        Best Regards\n\thaizhong\n'))

    html_content = f"""
<html>
<head>
<style>
    body {{
        font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
        background-color: #f9f9f9;
        color: #333;
        line-height: 1.6;
    }}
    .styled-table {{
        border-collapse: collapse;
        margin: 20px 0;
        font-size: 14px;
        width: 100%;
        box-shadow: 0 0 20px rgba(0,0,0,0.1);
    }}
    .styled-table th {{
        background-color: #009879;
        color: #ffffff;
        padding: 10px;
        text-align: center;
    }}
    .styled-table td {{
        padding: 8px;
        text-align: center;
    }}
    .styled-table tr:nth-child(even) {{
        background-color: #f3f3f3;
    }}
    .styled-table tr:hover {{
        background-color: #f1f1f1;
    }}
</style>
</head>
<body>
    <h2 style="color: #009879;">{Subject}</h2>
    <p>Dear Mr./Ms.,</p>
    <p>{content_1}<br>{content_2}</p>
    {df_to_html_clean(df)}
    <p>Detailed Report</p>
    {df_to_html_clean(df2)}
    <p>Best Regards,<br><b>Haizhong</b></p>
</body>
</html>
"""


    msg.attach(MIMEText(html_content, "html"))
    msg.set_unixfrom('LiWenxing')





    
    if to_all:
        msg['To'] = email.utils.formataddr(('bianhaizhong', bianhaizhong))
        msg['To'] = email.utils.formataddr(("xupeng", xupeng))
        msg['To'] = email.utils.formataddr(("renguochun", renguochun))
        msg['To'] = email.utils.formataddr(("zhangguangyao", zhangguangyao))
    else:
        msg['To'] = email.utils.formataddr(('haizhong.bian@cn.bosch.com', bianhaizhong))
    
    msg['From'] = email.utils.formataddr(('BianHaizhong', bianhaizhong))
    msg['Subject'] = Subject 

    server = smtplib.SMTP(servername)
    server.set_debuglevel(1)
    try:
        server.set_debuglevel(True)

        # identify ourselves, prompting server for supported features
        server.ehlo()

        # If we can encrypt this session, do it
        if server.has_extn('STARTTLS'):
            server.starttls()
            server.ehlo() # re-identify ourselves over TLS connection

        server.login(username, NT_PASSWORD)
        if to_all:
            server.sendmail('haizhong.bian@cn.bosch.com', [bianhaizhong,xupeng,renguochun,zhangguangyao], msg.as_string())
        else:
            server.sendmail('haizhong.bian@cn.bosch.com', [bianhaizhong], msg.as_string())
    finally:
        server.quit()

if __name__ == "__main__":
    send_report(Subject="test_sending_mail", to_all=False)