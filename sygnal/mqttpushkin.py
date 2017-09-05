# -*- coding: utf-8 -*-
# Copyright 2017 Matthias Kesler
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function, unicode_literals

import logging

import json
import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqtt_publish

from . import Pushkin
from .gcmpushkin import GcmPushkin
from .exceptions import PushkinSetupException


logger = logging.getLogger(__name__)

TLS_CA_FILE = '/etc/ssl/certs/ca-certificates.crt'

class MqttPushkin(Pushkin):

    def __init__(self, name):
        super(MqttPushkin, self).__init__(name)
        self.client = None

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        logger.info("Connected to Broker with result code " + str(rc))

    def setup(self, ctx):
        self.db = ctx.database

        self.broker = self.getConfig('broker_host')
        if not self.broker:
            raise PushkinSetupException("No Broker is set in config")

        self.username = self.getConfig('broker_user')
        self.password = self.getConfig('broker_pass')
        if not self.username or not self.password:
            logger.warn("No Credentials for Broker set in config")

        self.push_prefix = self.getConfig('push_prefix')
        if not self.push_prefix:
            raise PushkinSetupException("No push prefix is set in config")

        if not self.push_prefix.endswith('/'):
            self.push_prefix += '/'

        self.use_tls = self.getConfig('use_tls')

        self.port = self.getConfig('broker_port')
        if not self.port:
            self.port = 8883 if self.use_tls else 1883

        if self.getConfig('daemonize'):
            logger.info("Start daemonized mqtt client")
            self.client = mqtt.Client()
            if self.username:
                self.client.username_pw_set(self.username, self.password)
            self.client.on_connect = MqttPushkin.on_connect

            if self.use_tls:
                self.client.tls_set(TLS_CA_FILE)

            self.client.connect_async(self.broker, self.port)
            self.client.loop_start()

    def dispatchNotification(self, n):
        pushkeys = [device.pushkey for device in n.devices if device.app_id == self.name]

        # for deduplication use the one of GcmPushkin. If this changes an overwrite is needed
        data = GcmPushkin.build_data(n)
        payload = json.dumps(data)

        logger.info("%r => %r", payload, pushkeys);

        if self.client:
            # one connection was setup to push the notifications
            logger.debug("Push via existent connection")
            for pushkey in pushkeys:
                logger.debug("Push to %s", pushkey)
                self.client.publish(
                    topic=self.push_prefix + pushkey,
                    payload=payload,
                    qos=1,
                    retain=True,
                )
        else:
            # there is no connection to publish the notifications
            logger.debug("Push via non-existent connection")
            msgs = [
                {'topic': self.push_prefix + pushkey,
                 'payload': payload,
                 'qos': 1,
                 'retain': True}
                for pushkey in pushkeys]

            mqtt_publish.multiple(msgs,
                hostname=self.broker,
                port=self.port,
                auth={'username': self.username, 'password': self.password},
                tls={'ca_certs':TLS_CA_FILE} if self.use_tls else None,
            )

        # TODO return failed pushes
        return []

    def shutdown(self):
        if self.client:
            self.client.loop_stop()
