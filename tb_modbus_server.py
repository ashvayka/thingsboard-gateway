import logging
import threading
from tb_modbus_transport_manager import TBModbusTransportManager
log = logging.getLogger(__name__)


class TBModbusServer():
    _POLL_PERIOD = 1000  # time in milliseconds
    _thread = None
    scheduler = None
    client = None
    _server = None

    def __init__(self, server, scheduler):
        self.scheduler = scheduler
        self._server = server
        self._dict_current_parameters = {}
        self._thread = threading.Thread(target=self._server_processor_thread)
        self._thread.daemon = True
        self._thread.start()

    def _server_processor_thread(self):
        self.client = TBModbusTransportManager(self._server["transport"])
        if len(self._server["devices"]) == 0:
            # should we make it warning? it would be easier to be found in logs
            log.warning("there are no devices to process")
        for device in self._server["devices"]:
            log.debug("adding polling job for device id {id}".format(id=device["unitId"]))
            device_check_data_changed = TBModbusTransportManager.get_parameter(device, "sendDataOnlyOnChange", False)
            device_attr_poll_period = TBModbusTransportManager.get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
            device_ts_poll_period = TBModbusTransportManager.get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)
            for ts in device["timeseries"]:
                self._process_message(ts, device_ts_poll_period, "ts", device_check_data_changed, device)
            for atr in device["attributes"]:
                self._process_message(atr, device_attr_poll_period, "atr", device_check_data_changed, device)

    def _process_message(self, item, device_poll_period, type_of_data, device_check_data_changed, device):
        poll_period = TBModbusTransportManager.get_parameter(item, "pollPeriod", device_poll_period) / 1000  # millis to seconds
        check_data_changed = TBModbusTransportManager.get_parameter(item, "sendDataOnlyOnChange", device_check_data_changed)
        self.scheduler.add_job(self._get_values_check_send_to_tb,
                               'interval',
                               seconds=poll_period,
                               args=(check_data_changed, item, type_of_data, device))

    def _get_values_check_send_to_tb(self, check_data_changed, config, type_of_data, device):
        result = self.client.get_data_from_device(config, device["unitId"])
        result = self._transform_answer_to_readable_format(result, config)
        # firstly we check if we need to check data change, if true then do it
        if not check_data_changed or self._check_ts_atr_changed(result, type_of_data, device, config):
            self._send_to_tb(result)
            # todo maybe here we should return result, not just send to tb?

    def _send_to_tb(self, data):
        # todo here we send data to thingsboard
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

    def write_to_server(self, config_file):
        with open(config_file, 'r') as f:
            log.debug("reading file")
        # connect to server if not connected
        # send data with lock() call?

    @staticmethod
    def _transform_answer_to_readable_format(result, item):
        if item.get("functionCode") == 1 or item.get("functionCode") == 2:
            # we use registerCount to slice off always empty unused bits
            # todo fix straight registerCount usage
            #result = result.bits[:item["registerCount"]]
            # todo this is hardcode, remove
            result = result.bits[:1]
        if item.get("functionCode") == 3 or item.get("functionCode") == 4:
            result = result.registers
            # this is the case where value is boolean and we send only one bit from it
            if item.get("bit"):
                position = 15 - item["bit"]  # 16 bits in 2 registers, reverse order
                # transform result to string representation of a bit sequence, add "0" to make it longer >16
                result = "0000000000000000" + str(bin(result)[2:])
                # get length of 16, then get bit, then cast it to int(0||1 from "0"||"1", then cast to boolean)
                return bool(int((result[len(result) - 16:])[15 - position]))
        # todo add byte order

        return result
# TODO dict.get returns False if key exists, but value == False, need to redone every usage without function call!!!
