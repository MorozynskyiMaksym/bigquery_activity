import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

json_account = 'C:/Users/User/Desktop/diplom/service-account.json'
credentials = service_account.Credentials.from_service_account_file(json_account)
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = 'cellular-way-460520-i0'



def extract_logs():
    log_query = """
                SELECT * FROM `logs_analytics_cost_monitoring.query_analysis`
                WHERE CAST(`timestamp` AS DATE) >= CURRENT_DATE()
                """
    data_from_gcp = pandas_gbq.read_gbq(log_query, use_bqstorage_api=True)
    return data_from_gcp

def send_email(df):
    if not df.empty:
        receiver_email = 'morozynskyimaksym@gmail.com'
        subject = "Alert: users who spent too much money"
        text = create_msg_for_email(df)
        send_email_real(text, subject, receiver_email)
    else:
        print("ℹ️ There are no abnormal checks")


def send_email_real(html_message, subject, to_email):
    sender_email = os.getenv("LOGIN")  
    sender_password = os.getenv("PASS")  

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = to_email

    html_part = MIMEText(html_message, "html")
    message.attach(html_part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, message.as_string())
        print("✅ Email sent successfully!")

    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def generate_html_table(df):
    html = '<table style="border: 1px solid black; border-collapse: collapse; width: 100%; font-family: sans-serif;">'
    html += '<tr><th style="border: 1px solid black; padding: 8px; background-color: #f0f0f0;">Користувач</th>'
    html += '<th style="border: 1px solid black; padding: 8px; background-color: #f0f0f0;">Використана сума</th>'
    html += '<th style="border: 1px solid black; padding: 8px; background-color: #f0f0f0;">Кількість запитів</th></tr>'
    for _, row in df.iterrows():
        html += f'<tr><td style="border: 1px solid black; padding: 8px;">{row["user"]}</td>'
        html += f'<td style="border: 1px solid black; padding: 8px;">{row["total_cost"]:.2f}</td>'
        html += f'<td style="border: 1px solid black; padding: 8px;">{row["total_query_amount"]}</td></tr>'
    html += '</table>'
    return html

def create_msg_for_email(df):
    body = f"Наступні користувачі мають аномально велику кількість витрат за сьогодні:\n\n"
    table = generate_html_table(df)
    return body + table

def analyze_logs(df):
    df_group = df.groupby('user').agg(
        total_query_amount=('query', 'size'),
        total_cost=('queryCostInUSD', 'sum'),
    ).reset_index()
    
    result = df_group.loc[df_group['total_cost'] > 0]
    print(result)
    return result

def analyze_gcp_activity():
    logs = extract_logs()
    analyzed_logs = analyze_logs(logs)
    send_email(analyzed_logs)

if __name__ == '__main__':
    analyze_gcp_activity()