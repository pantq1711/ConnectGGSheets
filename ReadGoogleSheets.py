import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
import schedule
import time
from datetime import timedelta, datetime

def job():
    # Thiết lập thông tin xác thực Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('C:\\ConnectGGSheets\\data\\secrect_key.json', scopes=scope)
    client = gspread.authorize(credentials)

    # Mở Google Sheet
    sheet = client.open('MySheets').sheet1  

    # Lấy dữ liệu từ Google Sheet
    data = sheet.get_all_values()

    # Tạo DataFrame từ dữ liệu
    df = pd.DataFrame(data[1:], columns=data[0])  

    # Kết nối tới MySQL database
    conn = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password='',
                                   database='')
    cursor = conn.cursor()

    # Chuyển dữ liệu từ DataFrame vào MySQL database
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO table (column_name1, column_name2, ...) VALUES (%s, %s, ...)", tuple(row))

    # Lưu thay đổi
    conn.commit()

    # Đóng kết nối
    cursor.close()
    conn.close()

run = schedule.every(1).hour.do(job)

while True:
    schedule.run_pending()

