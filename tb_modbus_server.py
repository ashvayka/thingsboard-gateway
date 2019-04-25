import logging
from threading import Thread, Lock
from tb_modbus_transport_manager import TBModbusTransportManager as Manager
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from queue import Queue, Empty
import time
import datetime
log = logging.getLogger(__name__)


class TBModbusServer(Thread):
    _POLL_PERIOD = 1000  # time in milliseconds
    _RUN_TIMEOUT = 0.1

    scheduler = None
    client = None
    _server = None

    def __init__(self, server, scheduler):
        super(TBModbusServer, self).__init__()
        self.devices_names = set()
        self.queue_write_to_device = Queue()
        self.scheduler = scheduler
        self._server = server
        self.lock = Lock()
        self._dict_current_parameters = {}
        self.daemon = True
        self._read_from_devices_jobs_add()
        self.start()

    def run(self):
        while True:
            try:
                item = self.queue_write_to_device.get()
                self.client.write_data_to_device(item)
            except Empty:
                time.sleep(self._RUN_TIMEOUT)

    def _read_from_devices_jobs_add(self):
        self.client = Manager(self._server["transport"])
        if len(self._server["devices"]) == 0:
            # should we make it warning? it would be easier to be found in logs
            log.warning("there are no devices to process")
        # todo add_job here with config.json to periodicaly write smth to
        for device in self._server["devices"]:
            log.debug("adding polling job for device id {id}".format(id=device["unitId"]))
            device_check_data_changed = Manager.get_parameter(device, "sendDataOnlyOnChange", False)
            device_attr_poll_period = Manager.get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
            device_ts_poll_period = Manager.get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)
            for ts in device["timeseries"]:
                self._process_message(ts, device_ts_poll_period, "ts", device_check_data_changed, device)
            for atr in device["attributes"]:
                self._process_message(atr, device_attr_poll_period, "atr", device_check_data_changed, device)
            self.devices_names.add(device["deviceName"])

    def _process_message(self, item, device_poll_period, type_of_data, device_check_data_changed, device):
        poll_period = Manager.get_parameter(item, "pollPeriod", device_poll_period) / 1000  # millis to seconds
        check_data_changed = Manager.get_parameter(item, "sendDataOnlyOnChange", device_check_data_changed)
        self.scheduler.add_job(self._get_values_check_send_to_tb,
                               'interval',
                               seconds=poll_period,
                               next_run_time=datetime.datetime.now(),
                               args=(check_data_changed, item, type_of_data, device))

    def _get_values_check_send_to_tb(self, check_data_changed, config, type_of_data, device):
        result = self.client.get_data_from_device(config, device["unitId"])
        result = self._transform_answer_to_readable_format(result, config)
        # firstly we check if we need to check data change, if true then do it
        if not check_data_changed or self._check_ts_atr_changed(result, type_of_data, device, config):
            self._send_to_tb(result)
            # todo maybe here we should return result, not just send to tb?

    def _send_to_tb(self, data):
        # todo send data to thingsboard
        # maybe method should be public
        log.info("++++++++++++++++++++++++++++++++++++++++++")
        log.info(data)
        log.info("++++++++++++++++++++++++++++++++++++++++++")

    def _check_ts_atr_changed(self, value, type_of_data, device, item):
        key = self._server["transport"]["host"] + "|" + str(self._server["transport"]["port"]) + "|" \
              + str(device["unitId"]) + "|" + type_of_data + "|" + str(item["address"])
        if self._dict_current_parameters.get(key) == value:
            log.debug("{type} value {val} related to device id {id} didn't change".format(type=type_of_data,
                                                                                          val=value,
                                                                                          id=device["unitId"]))
            return False
        else:
            log.debug("{type} value {val} related to device id {id} changed".format(type=type_of_data,
                                                                                    val=value,
                                                                                    id=device["unitId"]))
            self._dict_current_parameters.update({key: value})
            return True

    def write_to_device(self, config):
        log.debug(config)
        with self.lock:
            self.queue_write_to_device.put(config)


# todo in read and write compare tag with byte order and smth?
    @staticmethod
    def _transform_request_to_device_format(request):
        log.debug(request)

    @staticmethod
    def _transform_answer_to_readable_format(result, config):
        # todo depending on byte order need to reshuffle result
        if config.get("functionCode") == 1 or config.get("functionCode") == 2:
            result = result.bits
            if "registerCount" in config:
                return result[:config["registerCount"]]
            else:
                return result[0]
        elif config.get("functionCode") == 3 or config.get("functionCode") == 4:
            byte_order = config.get("byteOrder")
            type_of_data = config.get("type")

            decoder = BinaryPayloadDecoder.fromRegisters(result.registers,
                                                         byteorder=Endian.Little)
            decoder.decode_32bit_int()
            result = result.registers
            # todo redone it,
            if "bit" in config:
                # with "bit" param we need only one bit, so there cannot be more then one pymodbus register
                result = result[0]
                position = 15 - config["bit"]  # reverse order
                # transform result to string representation of a bit sequence, add "0" to make it longer >16
                result = "0000000000000000" + str(bin(result)[2:])
                # get length of 16, then get bit, then cast it to int(0||1 from "0"||"1", then cast to boolean)
                return bool(int((result[len(result) - 16:])[15 - position]))
        return result
# todo write response processing?
