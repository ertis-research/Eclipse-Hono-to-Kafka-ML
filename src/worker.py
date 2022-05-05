from datetime import datetime
import json
import time
from sqlite3 import Time
from threading import Thread, Event

from proton import ConnectionException
from connections import connectWithHonoByAMQP, connectWithKafka
from database import findOneDevice, updateLastValue
from schemas import DeviceFields, RequiredValueFields, StateOptions, TenantFields
from proton._exceptions import Timeout
import numpy as np

calculatedValues = [ "$year", "$month", "$day", "$hour", "$minute", "$second", "$microsecond" ]

def format_kafka(number, format):
    return eval("np." + format)(round(float(number), 2))

#Modificar esto, permitiendo elegir el tipo de los elementos, que elementos quiere actuales del tiempo y cuales quiere almacenar
def send_last_value_to_kafkaml(producer, device, lastValue):
    ts = datetime.now()
    items = []
    sendData = True
    if (lastValue != None and DeviceFields.required_values.value in device): 
        for value in device[DeviceFields.required_values.value]:
            v = 0
            name = value[RequiredValueFields.nameValue.value]
            if(name in calculatedValues):
                v = eval("ts." + name.replace("$", ""))
            else:
                if RequiredValueFields.nameValue.value in lastValue and value[RequiredValueFields.nameValue.value] != None:
                    v = value[RequiredValueFields.nameValue.value]
                else:
                    print("lack of information")
                    sendData = False
                    return
            items.append(format_kafka(v, value[RequiredValueFields.format.value]))
    print(items)
    #items = np.array([format_kafka(ts.year), format_kafka(ts.month), format_kafka(ts.day), format_kafka(ts.hour), format_kafka(ts.minute), format_kafka(temp), format_kafka(hum)], dtype=np.float64)
    if(sendData == True):
        print("aa")
        items = np.array(items)
        print("bb")
        producer.send(device[DeviceFields.kafka_topic.value], items.tobytes())
        print("cc")
        producer.flush()
        print("Enviando last value a kafkaml. DeviceId: " + device[DeviceFields.device_id.value] + ", Last value: ")

# https://codereview.stackexchange.com/a/201760
def find_by_key(data, target):
    for key, value in data.items():
        if key == target:
            return value
        elif isinstance(value, dict):
            res = find_by_key(value, target)
            if res != None: return res
    return None


def extractLastValueFromMsg(msg, device):
    jsonBody = json.loads(msg)
    msg_path = None
    msg_value = None
    if "path" in jsonBody: msg_path = jsonBody["path"] 
    if "value" in jsonBody: msg_value = jsonBody["value"]
    res = {}

    if (device != None and DeviceFields.required_values.value in device and msg_path != None and msg_value != None):
        # COMPROBAR SI EL PATH CONTIENE EL NOMBRE, SINO ENTONCES COGER DIRECTAMENTE BODY.VALUE Y BUSCAR EL NOMBRE
        # COMO KEY. TENIENDO YA UNO DE LOS DOS BUSCAR HASTA ENCONTRAR UNA KEY VALUE.
        for value in device[DeviceFields.required_values.value]:
            newValue = msg_value
            nameValue = value[RequiredValueFields.nameValue.value]
            
            if not msg_path.endswith(nameValue) and "/" + nameValue + "/" not in msg_path: #No esta en el path
                newValue = find_by_key(msg_value, nameValue)
            
            if isinstance(newValue, dict):
                newValue = find_by_key(newValue, "value")
                if isinstance(newValue, dict): return
            
            if newValue != None: res[nameValue] = newValue 
    
    return res

            

#def getDeviceFromTenant(tenant, deviceId):
    #res = [device for device in tenant[TenantFields.devices.value] if device[DeviceFields.device_id.value]==deviceId]
    #return res[0] if (len(res) > 0) else None

def handle_message(msg, active_devices, tenantId, kafka_producer):
    if(msg and msg.body and msg.annotations):
        deviceId = msg.annotations.get(DeviceFields.device_id.value)
        if deviceId != None and deviceId in active_devices:
            device = findOneDevice(tenantId, deviceId)
            lastValue = extractLastValueFromMsg(msg.body, device)
            send_last_value_to_kafkaml(kafka_producer, device, lastValue)
            print("SEND")


class Worker(Thread):

    def __init__(self, tenant):
        super().__init__()
        self.tenant = tenant
        self._stop_event = Event()
        self.producer = connectWithKafka(self.tenant)
        self.active_devices = []

    def stop(self):
        self._stop_event.set()
        print("[Log " + self.tenant[TenantFields.tenant_id.value] + "]: Stopped")

    def stopped(self):
        return self._stop_event.is_set()

    def addDevice(self, device):
        if device[DeviceFields.state.value] == StateOptions.active.value:
            self.active_devices.append(device[DeviceFields.device_id.value])
    
    def removeDevice(self, deviceId, pop):
        if deviceId != None and deviceId in self.active_devices:
            if pop == True: self.active_devices.remove(deviceId)
            

    def run(self):
        try:
            conn, receiver = connectWithHonoByAMQP(self.tenant)
        except ConnectionException:
            print("ERROR CONEXION")
            conn, receiver = connectWithHonoByAMQP(self.tenant)

        for device in self.tenant[TenantFields.devices.value]:
            self.addDevice(device)

        print("comienza")
        while not self.stopped():
            try:
                msg = receiver.receive(timeout=5)
                handle_message(msg, self.active_devices, self.tenant[TenantFields.tenant_id.value], self.producer)
            except Timeout:
                print("timeout")
            except ConnectionException:
                print("ERROR CONEXION")
                conn, receiver = connectWithHonoByAMQP(self.tenant)

        print("close")

        for deviceId in self.active_devices.keys():
            self.removeDevice(deviceId, False)

        receiver.close()
        conn.close()
        self.producer.close()
        

