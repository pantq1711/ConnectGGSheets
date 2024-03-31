import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector

# Thiết lập thông tin xác thực Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name('C:\Connect\data\secrect_key.json', scopes = scope)
client = gspread.authorize(credentials)

# Mở Google Sheet
sheet = client.open('Your Google Sheet Name').sheet1  

# Lấy dữ liệu từ Google Sheet
data = sheet.get_all_values()

# Tạo DataFrame từ dữ liệu
df = pd.DataFrame(data[1:], columns=data[0])  

# Kết nối tới MySQL database
conn = mysql.connector.connect(host='your_host',
                               user='your_user',
                               password='your_password',
                               database='your_database')
cursor = conn.cursor()

# Chuyển dữ liệu từ DataFrame vào MySQL database
for index, row in df.iterrows():
    cursor.execute("INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)", tuple(row))

# Lưu thay đổi
conn.commit()

# Đóng kết nối
cursor.close()
conn.close()
