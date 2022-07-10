import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="admin",
  password="Killzone@2"
)
print(mydb)

mycursor = mydb.cursor()
mycursor.execute("SELECT table_name FROM information_schema.tables where table_schema = 'schipol_api'")
myresult = mycursor.fetchall()
for x in myresult:
  print(x)