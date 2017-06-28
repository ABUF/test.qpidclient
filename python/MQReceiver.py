#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import json
from qpid.messaging import *


def check_json_value(string):
    print('------------------------------------------')
    try:
        json.loads(string)
    except Exception as e:
        print(e)
    else:
        print('json string is correct')

if __name__ == "__main__":
    if len(sys.argv) < 3:
	print 'args: host queueid'
        sys.exit(-1)
    host, queueid = sys.argv[1:]
    print 'broke ip {}, queue id {}'.format(host, queueid)
    appid = queueid.split('.')[0]
    port = 4703
    broker = "{}/xxxx@{}:{}".format(appid, host, port)
    conn_options = {
                    'transport'               : 'ssl',
                    #'ssl_keyfile'             : "ssl_cert_file/MSP.Key.pem",
                    #'ssl_certfile'            : "ssl_cert_file/MSP.pem.cer",
                    #'ssl_trustfile'           : "ssl_cert_file/Wireless Root CA.pem.cer",
                    'ssl_keyfile'             : "../cert/user.pem",
                    'ssl_certfile'            : "../cert/user.pem.cer",
                    'ssl_trustfile'           : "../cert/root.pem.cer",
                    'ssl_skip_hostname_check' : True,
                    }
    connection = Connection(broker, **conn_options)

    try:
        connection.open()
        session = connection.session()
        receiver = session.receiver(queueid)
        print "session create success"
        while True:
            message = receiver.fetch()
            check_json_value(message.content)
            print "%r" % message.content
            session.acknowledge()

    except MessagingError, m:
        print "MessagingError", m
    connection.close()
