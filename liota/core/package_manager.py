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

import logging
import inspect
from threading import Thread, Lock
from Queue import Queue
import ConfigParser
import os
from time import sleep
import imp
import re
from abc import ABCMeta, abstractmethod
import hashlib

from liota.utilities.utility import LiotaConfigPath

log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.warning("Package manager is not supposed to run alone")
    import liota.core.package_manager
    log.debug("MainThread is going to exit...")
    exit()

is_package_manager_initialized = False

package_message_queue = None
package_messenger_thread = None
package_thread = None
package_lock = None
package_path = None

# Parse Liota configuration file
config = ConfigParser.RawConfigParser()
fullPath = LiotaConfigPath().get_liota_fullpath()
if fullPath != '':
    try:
        if config.read(fullPath) != []:
            try:
                package_path = config.get('PKG_PATH', 'pkg_path')
            except ConfigParser.ParsingError, err:
                log.error('Could not parse log config file')
        else:
            raise IOError('Cannot open configuration file ' + fullPath)
    except IOError:
        log.error('Could not open config file')
else:
    # missing config file
    log.error('liota.conf file missing')

class ResourceRegistry:
    """
    ResourceRegistry is a wrapped structure for Liota packages to register
    resources and find resources registered by other packages.
    """

    def __init__(self):
        self._registry = {}

    def register(self, identifier, ref):
        if identifier in self._registry:
            raise KeyError("Conflicting resource identifier: " + identifier)
        self._registry[identifier] = ref

    def deregister(self, identifier):
        del self._registry[identifier]

    def get(self, identifier):
        return self._registry[identifier]

    def has(self, identifier):
        return identifier in self._registry

class LiotaPackage:
    """
    LiotaPackage is ABC (abstract base class) of all package classes.
    Here it should define abstract methods that developers should implement.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def run(self, registry):
        raise NotImplementedError

    @abstractmethod
    def clean_up(self):
        raise NotImplementedError

#---------------------------------------------------------------------------
# This method calculates SHA-1 checksum of file.
# May raise IOError upon "open"

def sha1sum(path_file):
    sha1 = hashlib.sha1()
    with open(path_file, "rb") as fp:
        while True:
            data = fp.read(65536) # buffer size
            if not data:
                break
            sha1.update(data)
    return sha1

class PackageRecord:
    """
    PackageRecord is instantiated for each package loaded.
    It contains necessary information of packages, including but may not be
    limited to file names, checksums, and refs to package class instances.
    These records make it possible to start and stop package class instances
    elegantly.
    """

    def __init__(self, file_name):
        self.file_name = file_name
        self._sha1 = None

        #-------------------------------------------------------------------
        # To guarantee successful garbage collection when record is removed,
        # this should be the ONLY variable to keep its reference.

        self._instance = None

    def set_instance(self, obj):
        if self._instance is not None:
            log.warning("Should not override instance of package class")
            return False
        self._instance = obj
        return True

    def get_instance(self):
        return self._instance

    def set_sha1(self, sha1):
        self._sha1 = sha1

    def get_sha1(self):
        return self._sha1

class PackageThread(Thread):
    """
    PackageThread should be instantiated only once.
    Its instance serves core functionalities of Liota package manager,
    including load/unload of packages, maintenance of loaded package records,
    etc.
    """

    def __init__(self):
        Thread.__init__(self)
        self._packages_loaded = {}
        self._resource_registry = ResourceRegistry()
        self.start()

    #-----------------------------------------------------------------------
    # This method will loop on message queue and select methods to call with
    # respect to commands received.

    def run(self):
        global package_lock
        while True:
            msg = package_message_queue.get()
            log.info("Got message in package messenger queue: " + " ".join(msg))
            if not isinstance(msg, tuple):
                raise RuntimeError

            # Switch on message content (command), determine what to do
            command = msg[0]
            if command in ["load", "unload", "delete", "update"]:
                with package_lock:
                    if len(msg) != 2:
                        raise ValueError("len(msg) is %d" % len(msg))
                    file_name = msg[1]
                    if command == "load":
                        package_record = self._package_load(file_name)
                        if package_record is not None:
                            self._packages_loaded[file_name] = package_record
                    elif command == "unload":
                        if self._package_unload(file_name):
                            del self._packages_loaded[file_name]
                    elif command == "delete":
                        pass # TODO
                    elif command == "update":
                        pass # TODO
            elif command == "check":
                pass # TODO

    #-----------------------------------------------------------------------
    # This method is called to load package into current Liota process using
    # file_name as package identifier.

    def _package_load(self, file_name):
        log.debug("Attempting to load package file: " + file_name)

        # Check if specified package is already loaded
        if file_name in self._packages_loaded:
            log.warning("Package already loaded: " + file_name)
            return None

        # Check if specified package exists
        c_slash = "/"
        if package_path.endswith(c_slash):
            c_slash = ""
        path_file = os.path.abspath(package_path + c_slash + file_name)
        if not os.path.isfile(path_file):
            log.error("Package file not found: " + path_file)
            return None

        # Read file and calculate SHA-1
        try:
            sha1 = sha1sum(path_file)
        except IOError:
            log.error("Could not open file: " + path_file)
            return None
        log.info("Loaded package file: %s (%s)" \
                % (path_file, sha1.hexdigest()))

        #-------------------------------------------------------------------
        # Following ugly lines do these:
        #       from file path  load module,
        #       from module     load class,
        #       with class      create instance (object),
        #   and call method run of created instance.

        module_loaded = imp.load_source(
                re.sub(r"\.", "_", file_name),
                path_file
            )
        log.info("Loaded module: " + module_loaded.__name__)
        klass = getattr(module_loaded, "PackageClass")
        package_record = PackageRecord(file_name)
        if not package_record.set_instance(klass()):
            log.error("Unexpected failure initializing package")
            return None
        package_record.get_instance().run(self._resource_registry)
        package_record.set_sha1(sha1)

        log.info("Package class from module %s is initialized" \
                % module_loaded.__name__)
        return package_record

    def _package_unload(self, file_name):
        log.debug("Attempting to unload package file: " + file_name)
        
        # Check if specified package is already loaded
        if not file_name in self._packages_loaded:
            log.warning("Could not unload package if not loaded: " + file_name)
            return False

        package_record = self._packages_loaded[file_name]
        assert(isinstance(package_record, PackageRecord))
        package_obj = package_record.get_instance()
        try:
            assert(isinstance(package_obj, LiotaPackage))
        except:
            print type(package_obj)
            print inspect.getmro(package_obj)
        package_obj.clean_up()

        log.info("Unloaded package file: " + file_name)
        return True

    def _package_delete(self, file_name):
        log.debug("Attempting to delete package file: " + file_name)
        pass # TODO
        log.info("Deleted package file: " + file_name)

    def _package_update(self, file_name):
        log.debug("Attempting to update package file: " + file_name)
        self._package_unload(file_name)
        self._package_load(file_name)
        log.info("Updated package file: " + file_name)

class PackageMessengerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        global package_thread
        global package_lock
        while True:
            if package_thread is not None:
                sleep(10)
                with package_lock:
                    if "test.py" in package_thread._packages_loaded:
                        pass
                        package_message_queue.put(("unload", "test.py"))
                    else:
                        package_message_queue.put(("load", "test.py"))
        # XXX

#---------------------------------------------------------------------------
# Initialization should occur when this module is imported for first time.
# This method create queues and spawns PackageThread, which will loop on
# commands grabbed from a queue and load/unload packages as requested in
# those commands.

def initialize():
    global is_package_manager_initialized
    if is_package_manager_initialized:
        log.debug("Package manager is already initialized")
        return

    # Validate package path
    global package_path
    if isinstance(package_path, basestring):
        if os.path.isdir(package_path):
            try:
                os.listdir(package_path)
            except OSError:
                package_path = None
                log.warning("Could not access package path")
                return
        else:
            package_path = None
            log.warning("Could not find package path")
            return

    # Will not initialize package manager if package path is mis-configured
    if package_path is None:
        log.error("Package manager failed because package path is invalid")
        return

    # Initialization of package manager
    global package_message_queue
    if package_message_queue is None:
        package_message_queue = Queue()
    global package_thread
    if package_thread is None:
        package_thread = PackageThread()
    global package_lock
    if package_lock is None:
        package_lock = Lock()

    # PackageMessengerThread should start last because it triggers actions 
    global package_messenger_thread
    if package_messenger_thread is None:
        package_messenger_thread = PackageMessengerThread()

    # Mark package manager as initialized
    is_package_manager_initialized = True
    log.info("Package manager is initialized")

# Initialization of this module
if not is_package_manager_initialized:
    initialize()

log.debug("Package manager module is imported")
