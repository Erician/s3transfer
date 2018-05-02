#encoding=utf-8
from email.mime.text import MIMEText
import smtplib
import configoperations
import logging
log = logging.getLogger('root')

def send_mail(content):
    tsCfg = configoperations.read_ts_cfg()
    if not (tsCfg.has_key('msg_from') and tsCfg.has_key('msg_to')):
        return
    pairs = tsCfg['msg_from'].split(':')
    msg_from = pairs[0]
    passwd = pairs[1]
    msg_to = tsCfg['msg_to']

    subject = "s3transfer 完成情况"                  # 主题
    msg = MIMEText(content)
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to
    try:
        s = smtplib.SMTP()  # 邮件服务器及端口号
        s.connect('smtp.163.com')
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        log.info('send mail to '+msg_to+' successfully')
    except smtplib.SMTPException, e:
        log.info(e)
        log.info('send mail to ' + msg_to + ' fail')
    finally:
        s.quit()

if __name__ == '__main__':
    pass