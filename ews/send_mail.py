import smtplib
import email.utils
from email.mime.text import MIMEText
from pathlib import Path
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

def send_report(Subject=None, content_1= "", content_2="", to_all= False):
    msg = MIMEText(f'Dear : Mr./Ms. \n {content_1}.\n {content_2}.\n        Best Regards\n\thaizhong\n')
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
    send_report(Subject="test_sending_mail", to_all=True)