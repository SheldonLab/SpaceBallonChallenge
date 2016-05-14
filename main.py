from serialComs import SIM808
#from databaseWrapper import DatabaseWrapper
import time
host = "localhost"
user = "root"
password = "moxie100"
database = (host, user, password)

card = SIM808()
#data = DatabaseWrapper(database)
while True:
    gps = card.get_gps_data()
    card.post_gps_data(gps)
    time.sleep(2)
