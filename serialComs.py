import serial
import time
import json


class SerialInterface:
    def __init__(self):
         self.con = serial.Serial('/dev/ttyAMA0', baudrate = 115200, timeout = 10)

    def send_command(self, command):
        self.con.write(command+'\r')
        time.sleep(2)
        sizeBytes = self.con.inWaiting()
        return self.con.read(sizeBytes)

    def close(self):
        self.con.close()
        return 1


class SIM808(SerialInterface):
    def __init__(self):
        SerialInterface.__init__(self)
        self.turn_on_gps()
        self.turn_on_http()

    def turn_on_gps(self):
        response = self.send_command('AT+CGNSPWR?')
        print response
        gpson = int(response[24])
        if not gpson:
            print self.send_command('AT+CGNSPWR=1')

    def turn_on_http(self):
        self.send_command('AT+SAPBR=3,1,"Contype", "GPRS"')
        self.send_command('AT+SAPBR=3,1,"APN","wholesale"')
        self.send_command('AT+SAPBR=1,1')
        self.send_command('AT+SAPBR=2,1')

    def get_gps_data(self):
        response = self.send_command('AT+CGNSINF')
        data = response.split(',')
        print data
        time_stamp = data[2]
        g = lambda x: 0 if x == '' else float(x)
        latitude   = g(data[3])
        longitude  = g(data[4])
        altitude   = g(data[5])
        ground_speed = g(data[6])
        ground_course = g(data[7])
        data = {'time': time_stamp,
                'latitude': latitude,
                'longitude': longitude,
                'altitude': altitude,
                'ground_speed': ground_speed,
                'ground_course': ground_course}

        return json.dumps(data)

    def post_gps_data(self, data):
        self.send_command('AT+HTTPINIT')
        self.send_command('AT+HTTPPARA="CID",1')
        self.send_command('AT+HTTPPARA="URL","http://52.91.240.90:8001"')
        self.send_command('AT+HTTPPARA="CONTENT","application/json"')
        self.send_command('AT+HTTPDATA='+str(len(data))+',10000')
        self.send_command(data)
        response = self.send_command('AT+HTTPACTION=1')
        print response
        self.send_command('AT+HTTPTERM')

