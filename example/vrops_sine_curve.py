# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------#
#  Copyright © 2015-2016 VMware, Inc. All Rights Reserved.                    #
#                                                                             #
#  Licensed under the BSD 2-Clause License (the “License”); you may not use   #
#  this file except in compliance with the License.                           #
#                                                                             #
#  The BSD 2-Clause License                                                   #
#                                                                             #
#  Redistribution and use in source and binary forms, with or without         #
#  modification, are permitted provided that the following conditions are met:#
#                                                                             #
#  - Redistributions of source code must retain the above copyright notice,   #
#      this list of conditions and the following disclaimer.                  #
#                                                                             #
#  - Redistributions in binary form must reproduce the above copyright        #
#      notice, this list of conditions and the following disclaimer in the    #
#      documentation and/or other materials provided with the distribution.   #
#                                                                             #
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"#
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  #
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE #
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE  #
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR        #
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF       #
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS   #
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN    #
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)    #
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF     #
#  THE POSSIBILITY OF SUCH DAMAGE.                                            #
# ----------------------------------------------------------------------------#

from liota.boards.gateway_dk300 import Dk300
from liota.dcc.vrops import Vrops
from liota.things.function import Function
from liota.transports.web_socket import WebSocket
import random

# getting values from conf file
config = {}
execfile('sampleProp.conf', config)

# some standard metrics for Linux systems
# agent classes for different IoT gateways
# agent classes for different data center components
# agent classes for different kinds of of devices, 'Things', connected to the gw
# we are showing here how to create a representation for a Thing in vROps but
# using the notion of RAM (because we have no connected devies yet)
# agent classes for different kinds of layer 4/5 connections from agent to DCC
# -------User defined functions for getting the next value for a metric --------
# usage of these shown below in main
# semantics are that on each call the function returns the next available value
# from the device or system associated to the metric.

import time
import math

def get_curve_value():
    return math.sin(time.time() / (1800) * math.pi)

#---------------------------------------------------------------------------
# In this example, we demonstrate how gateway health and some simluated data 
# can be directed to vrops, VMware's data center component using Liota.
# The program illustrates the ease of use Liota brings to IoT application developers.

if __name__ == '__main__':

    # create a data center object, vROps in this case, using websocket as a transport layer
    # this object encapsulates the formats and protocols neccessary for the agent to interact with the dcc
    # UID/PASS login for now.
    vrops = Vrops(config['vROpsUID'], config['vROpsPass'], WebSocket(url=config['WebSocketUrl']))

    # create a gateway object encapsulating the particulars of a gateway/board
    # argument is the name of this gateway
    gateway = Dk300(config['Gateway1Name'])

    # resister the gateway with the vrops instance
    # this call creates a representation (a Resource) in vrops for this gateway with the name given
    vrops_gateway = vrops.register(gateway)
    
    if vrops_gateway.registered:
        pass
    else:
        print "vROPS resource not registered successfully"


    # Here we are showing how to create a device object, registering it in vrops, and setting properties on it
    # Since there are no attached devices are as simulating one by considering RAM as separate from the gateway
    # The agent makes possible many different data models
    # arguments:
    #        device name
    #        Read or Write
    #        another Resource in vrops of which the should be the child of a parent-child relationship among Resources
    curve = Function("Curve", 'Read', gateway)
    vrops_curve = vrops.register(curve)
    if vrops_curve.registered:
        for item in config['Device1PropList']:
            for key, value in item.items():
                vrops.set_properties(key, value, vrops_curve)
        curve_value = vrops.create_metric(vrops_curve, "Sine_Function_Value", unit=None, sampling_interval_sec=30, sampling_function=get_curve_value)
        curve_value.start_collecting()
    else:
        print "vROPS resource not registered successfully"

