from databaseWrapper import DatabaseWrapper


class DataUpload(DatabaseWrapper):

    def __init__(self, data):
        database = ("localhost", "root", "moxie100")
        self.data = data
        DatabaseWrapper.__init__(self, database)
        self.database_name = "gpsdata"
        self.table_name = "gps"
        self.insert_data()

    def insert_data(self):
        self.insert_into_database(self.database_name,
                                  self.table_name,
                                  ['timestamp', 'latitude', 'longitude'],
                                  [self.data['time'], self.data['lat'], self.data['long']])
        return True
