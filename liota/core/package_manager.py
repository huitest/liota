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
import imp
import os
import stat
import re
import hashlib
import ConfigParser
from threading import Thread, Lock
from Queue import Queue
from time import sleep
from abc import ABCMeta, abstractmethod

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
package_messenger_pipe = None

# Parse Liota configuration file
config = ConfigParser.RawConfigParser()
fullPath = LiotaConfigPath().get_liota_fullpath()
if fullPath != '':
    try:
        if config.read(fullPath) != []:
            try:
                package_path = os.path.abspath(
                        config.get('PKG_CFG', 'pkg_path')
                    )
                package_messenger_pipe = os.path.abspath(
                        config.get('PKG_CFG', 'pkg_msg_pipe')
                    )
            except ConfigParser.ParsingError, err:
                log.error('Could not parse log config file')
        else:
            raise IOError('Cannot open configuration file ' + fullPath)
    except IOError:
        log.error('Could not open config file')
else:
    # missing config file
    log.error('liota.conf file missing')

class ResourceRegistryPerPackage:
    """
    ResourceRegistryPerPackage creates temporary objects for packages while
    loading, so when they register their resource refs, we can keep track of
    them in resource registry and automatically remove them later when these
    packages are unloaded.
    """

    def __init__(self, outer, package_name):
        self._outer = outer
        self._package_name = package_name

    def register(self, identifier, ref):
        self._outer.register(identifier, ref, self._package_name)

    def get(self, identifier):
        return self._outer.get(identifier)

    def has(self, identifier):
        return self._outer.has(identifier)

class ResourceRegistry:
    """
    ResourceRegistry is a wrapped structure for Liota packages to register
    resources and find resources registered by other packages.
    """

    def __init__(self):
        self._registry = {} # key: resource name, value: resource ref
        self._packages = {} # key: package name, value: list of resource names

    def register(self, identifier, ref, package_name=None):
        if identifier in self._registry:
            raise KeyError("Conflicting resource identifier: " + identifier)
        self._registry[identifier] = ref
        if package_name:
            if not package_name in self._packages:
                self._packages[package_name] = []
            self._packages[package_name].append(identifier)

    def deregister(self, identifier):
        del self._registry[identifier]

    def get(self, identifier):
        return self._registry[identifier]

    def has(self, identifier):
        return identifier in self._registry

    #-----------------------------------------------------------------------
    # This method generate a package specific registry object, so when they
    # register their resource refs, we keep track of them and can deregister
    # them automatically if package is unloaded.

    def get_package_registry(self, package_name):
        return ResourceRegistryPerPackage(self, package_name)

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
        self._file_name = file_name
        self._ext = None
        self._sha1 = None
        self._dependents = {} # key: dependent name, value: None

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

    def set_ext(self, ext):
        self._ext = ext

    def get_ext(self):
        return self._ext

    def get_dependents(self):
        return self._dependents.keys()

    def add_dependent(self, file_name):
        self._dependents[file_name] = None

    def del_dependent(self, file_name):
        del self._dependents[file_name]

class PackageThread(Thread):
    """
    PackageThread should be instantiated only once.
    Its instance serves core functionalities of Liota package manager,
    including load/unload of packages, maintenance of loaded package records,
    etc.
    """

    def __init__(self, name=None):
        Thread.__init__(self, name=name)

        global package_path

        self._packages_loaded = {} # key: package name, value: PackageRecord obj
        self._resource_registry = ResourceRegistry()
        self._resource_registry.register("package_conf", package_path)
        self.start()

    #-----------------------------------------------------------------------
    # This method is used to handle listing commands

    def _cmd_handler_list(self, parameter):
        if parameter == "packages" or parameter == "pkg":
            log.warning("List of packages - \n\t%s" \
                    % "\n\t".join(sorted(
                            self._packages_loaded.keys()
                        ))
                )
            return
        if parameter == "resources" or parameter == "res":
            log.warning("List of resources - \n\t%s" \
                    % "\n\t".join(sorted(
                            self._resource_registry._registry.keys()
                        ))
                )
            return
        if parameter == "threads" or parameter == "th":
            import threading
            
            log.warning("Active threads - \n\t%s" \
                    % "\n\t".join(map(
                        lambda tref: "%s: %016x %s %s" % (
                                tref.name,
                                tref.ident,
                                type(tref).__name__.split(".")[-1],
                                tref.isAlive()
                            ),
                        sorted(
                            threading.enumerate(),
                            key=lambda tref: tref.ident
                        )
                    ))
                )
            return
        log.warning("Unsupported list")

    #-----------------------------------------------------------------------
    # This method is used to handle statistical commands

    def _cmd_handler_stat(self, parameter):
        if parameter == "metrics" or parameter == "met":
            from liota.core.metric_handler \
                import event_ds, collect_queue, send_queue, \
                       CollectionThreadPool, collect_thread_pool

            stats = ["n/a", "n/a", "n/a", "n/a"]
            if isinstance(event_ds, Queue):
                stats[0] = str(event_ds.qsize())
            if isinstance(send_queue, Queue):
                stats[1] = str(send_queue.qsize())
            if isinstance(collect_queue, Queue):
                stats[2] = str(collect_queue.qsize())
            if isinstance(collect_thread_pool, CollectionThreadPool):
                stats[3] = collect_thread_pool.get_stats_working()[0]
            log.warning(("Number of metrics in - \n\t" \
                    + "Waiting queue: %s\n\t" \
                    + "Sending queue: %s\n\t" \
                    + "Collecting queue: %s\n\t" \
                    + "Collecting threads: %s"
                ) % tuple(stats))
            return
        if parameter == "collection_threads" or parameter == "col":
            from liota.core.metric_handler \
                import CollectionThreadPool, collect_thread_pool

            stats = ["n/a", "n/a", "n/a", "n/a"]
            if isinstance(collect_thread_pool, CollectionThreadPool):
                stats = map(
                        lambda n: str(n), 
                        collect_thread_pool.get_stats_working()
                    )
            log.warning(("Status of collection threads - \n\t" \
                    + "Collecting: %s\n\t" \
                    + "Alive: %s\n\t" \
                    + "Pool: %s\n\t" \
                    + "Capacity: %s"
                ) % tuple(stats))
            return
        if parameter == "threads" or parameter == "th":
            import threading

            log.warning("Count of active threads: %d" \
                    % threading.active_count())
            return
        log.warning("Unsupported stat")

    #-----------------------------------------------------------------------
    # This method will loop on message queue and select methods to call with
    # respect to commands received.

    def run(self):
        global package_lock

        # Listen on message queue for commands
        # Other packages are loaded here according to commands received
        while True:
            msg = package_message_queue.get()
            log.info("Got message in package messenger queue: %s" \
                    % " ".join(msg))
            if not isinstance(msg, tuple) and not isinstance(msg, list):
                raise TypeError(type(msg))

            # Switch on message content (command), determine what to do
            command = msg[0]
            if command in ["load", "unload", "delete", "reload", "update"]:
                #-----------------------------------------------------------
                # Use these commands to handle package management tasks

                with package_lock:
                    if len(msg) != 2:
                        log.warning("Invalid format of command: %s" % command)
                        continue
                    file_name = msg[1]
                    if command == "load":
                        self._package_load(file_name)
                    elif command == "unload":
                        self._package_unload(file_name)
                    elif command == "delete":
                        pass # TODO
                    elif command == "reload":
                        self._package_reload(file_name)
                    elif command == "update":
                        self._package_update(file_name)
                    else: # should not happen
                        raise RuntimeError("Command category error")
            elif command == "list":
                with package_lock:
                    if len(msg) != 2:
                        log.warning("Invalid format of command: %s" % command)
                        continue
                    self._cmd_handler_list(msg[1])
            elif command == "stat":
                with package_lock:
                    if len(msg) != 2:
                        log.warning("Invalid format of command: %s" % command)
                        continue
                    self._cmd_handler_stat(msg[1])
            else:
                log.warning("Unsupported command is dropped")

    #-----------------------------------------------------------------------
    # This method is called to load package into current Liota process using
    # file_name (no_ext) as package identifier.

    def _package_load(self, file_name, ext_forced=None, check_stack=None):
        log.debug("Attempting to load package: %s" % file_name)

        # Check if specified package is already loaded
        if file_name in self._packages_loaded:
            log.warning("Package already loaded: %s" % file_name)
            return None

        # Check if specified package exists
        c_slash = "/"
        if package_path.endswith(c_slash):
            c_slash = ""
        path_file = os.path.abspath(package_path + c_slash + file_name)

        file_ext = None
        extensions = ["py", "pyc", "pyo"]
        prompt_ext_all = "py[co]?"
        if not ext_forced:
            for file_ext_ind in extensions:
                if os.path.isfile(path_file + "." + file_ext_ind):
                    file_ext = file_ext_ind
                    break
        else:
            if os.path.isfile(path_file + "." + ext_forced):
                file_ext = ext_forced
        if not file_ext:
            if not ext_forced:
                log.error("Package file not found: %s" \
                        % (path_file + "." + prompt_ext_all))
            else:
                log.error("Package file not found: %s" \
                        % (path_file + "." + ext_forced))
            return None
        path_file_ext = path_file + "." + file_ext
        log.debug("Package file found: %s" % path_file_ext)

        # Read file and calculate SHA-1
        try:
            sha1 = sha1sum(path_file_ext)
        except IOError:
            log.error("Could not open file: %s" % path_file_ext)
            return None
        log.info("Loaded package file: %s (%s)" \
                % (path_file_ext, sha1.hexdigest()))

        #-------------------------------------------------------------------
        # Following sections do these:
        #   1)     from file path  load module,
        #   2)     from module     load class,
        #   3)     with class      create instance (object),
        #   4) and call method run of created instance.

        #-------------------------------------------------------------------
        # Attempt to load package module from file.
        # Supported file types are source files (.py) with highest priority,
        #                          compiled files (.pyc),
        #                      and optimized compiled files (.pyo).
        # Having .py files to have highest priority guarantees that coming
        # packages in .py format can override compiled files of its previous
        # version.

        module_loaded = None
        module_name = re.sub(r"\.", "_", file_name)
        if file_ext in ["py"]:
            module_loaded = imp.load_source(
                    module_name,
                    path_file_ext
                )
        elif file_ext in ["pyc", "pyo"]:
            module_loaded = imp.load_compiled(
                    module_name,
                    path_file_ext
                )
        else: # should not happen
            raise RuntimeError("File extension category error")
        log.debug("Loaded module: %s" % module_loaded.__name__)

        #-------------------------------------------------------------------
        # Acquire dependency list and recursively load them.
        # If any dependency fails to load, current package will not load.

        if hasattr(module_loaded, "dependencies"):
            dependencies = getattr(module_loaded, "dependencies")
            if not isinstance(dependencies, list):
                log.error("Mal-formatted list of dependencies in module %s" \
                        % module_loaded.__name__)
                return None

            if len(dependencies) > 0:
                log.info("Package %s depends on: %s" \
                        % (file_name, " ".join(dependencies)))
                if not isinstance(check_stack, list):
                    check_stack = []
                check_stack.append(file_name)
                for dependency in dependencies:
                    if dependency in check_stack:
                        log.error("%s is not loaded, because %s depends on it" \
                                % (file_name, dependency))
                        check_stack.pop()
                        return None
                    if not dependency in self._packages_loaded:
                        self._package_load(dependency, check_stack=check_stack)
                    if not dependency in self._packages_loaded:
                        log.error("%s is not loaded, because %s failed to load"\
                                % (file_name, dependency))
                        check_stack.pop()
                        return None
                    
                    # Add dependent record
                    dep_record = self._packages_loaded[dependency]
                    assert(isinstance(dep_record, PackageRecord))
                    dep_record.add_dependent(file_name)
                check_stack.pop()
                log.debug("Dependency check of package %s is complete" \
                        % file_name)

        # Get package class from module and instantiate it
        klass = getattr(module_loaded, "PackageClass")
        package_record = PackageRecord(file_name)
        if not package_record.set_instance(klass()):
            log.error("Unexpected failure initializing package")
            return None
        try: # Run created instance
            package_record.get_instance().run(
                    self._resource_registry.get_package_registry(file_name)
                )
        except Exception as er:
            log.error("Exception in initialization: %s" % str(er))
            return None
        package_record.set_sha1(sha1)
        package_record.set_ext(file_ext)
        self._packages_loaded[file_name] = package_record

        log.info("Package class from module %s is initialized" \
                % module_loaded.__name__)
        return package_record

    #-----------------------------------------------------------------------
    # This method is called to unload package using its file_name (no ext).

    def _package_unload(self, file_name, track_list=None):
        log.debug("Attempting to unload package: %s" % file_name)
        
        # Check if specified package is already loaded
        if not file_name in self._packages_loaded:
            log.warning("Could not unload package - not loaded: %s" \
                    % file_name)
            return False

        package_record = self._packages_loaded[file_name]
        assert(isinstance(package_record, PackageRecord))

        # Stop all dependents, before making any change to current package
        dependents = package_record.get_dependents()
        
        if len(dependents) > 0:
            log.info("Package %s is depended by: %s" \
                    % (file_name, " ".join(dependents)))
            for dependent in dependents:
                if dependent in self._packages_loaded:
                    self._package_unload(dependent, track_list=track_list)
                if dependent in self._packages_loaded:
                    log.error("%s is still alive, because %s failed to unload" \
                        % (file_name, dependent))
                    return False
                package_record.del_dependent(dependent)
            log.debug("Dependency check of package %s is complete" \
                % file_name)

        package_obj = package_record.get_instance()
        if not isinstance(package_obj, LiotaPackage):
            raise TypeError(type(package_obj))

        # Deregister resources
        # Unload should proceed no matter deregistration succeeds or not
        if file_name in self._resource_registry._packages:
            for identifier in self._resource_registry._packages[file_name]:
                self._resource_registry.deregister(identifier)
            del self._resource_registry._packages[file_name]
            log.debug("Deregistered resource refs for package: %s" \
                    % file_name)
        else:
            log.warning("Could not deregister resource refs for package: %s" \
                    % file_name)

        # Clean-up
        try:
            package_obj.clean_up()
        except Exception as er:
            log.error("Exception in clean-up: %s" % er)
        if isinstance(track_list, list):
            track_list.append((file_name, package_record.get_ext()))
        del self._packages_loaded[file_name]

        log.info("Unloaded package: %s" % file_name)
        return True

    def _package_delete(self, file_name):
        log.debug("Attempting to delete package: %s" % file_name)
        pass # TODO
        log.info("Deleted package: %s" % file_name)

    #-----------------------------------------------------------------------
    # This method is called to reload package.
    # We keep track of full file name (with ext) when unloading, so reload
    # will always load exactly that same file, even if a different higher-
    # priority source file is added before reload.

    def _package_reload(self, file_name):
        log.debug("Attempting to reload package: %s" % file_name)

        # Check if specified package is already loaded
        if not file_name in self._packages_loaded:
            log.warning("Could not reload package - not loaded: %s" \
                    % file_name)
            return False
        
        # Logic of reload
        track_list = []
        if self._package_unload(file_name, track_list=track_list):
            package_record = None
            track_list.reverse()
            log.info("Packages will be reloaded: %s" \
                    % " ".join(
                            map(lambda item: item[0], track_list)
                        )
                )
            for track_item in track_list:
                if track_item[0] in self._packages_loaded:
                    continue
                temp_record = \
                    self._package_load(track_item[0], ext_forced=track_item[1])
                if temp_record is not None:
                    if track_item[0] == file_name:
                        package_record = temp_record
                    log.info("Reloaded package: %s" % file_name)
                else:
                    log.error("Unloaded but could not reload package: %s" \
                            % file_name)
            if not package_record is None:
                return package_record
            else:
                return False
        else:
            log.warning("Could not unload package: %s" % file_name)
        return False

    #-----------------------------------------------------------------------
    # This method is called to update package.
    # We keep track of full file name (without ext) when unloading.
    # The difference between this method and _package_reload is:
    #   1)  If target package is not loaded, this method tries to load it.
    #   2)  For all packages involved in update, this method calls 
    #       _package_load to look for source files and compiled files in our
    #       preferred priority order, so updated source file can be used to
    #       update target package even if it was loaded using compiled file.

    def _package_update(self, file_name):
        log.debug("Attempting to update package: %s" % file_name)
        
        # Check if specified package is already loaded
        if not file_name in self._packages_loaded:
            log.info("Package is not loaded, will try to load: %s" \
                    % file_name)
            return self._package_load(file_name)
        
        # Logic of reload
        track_list = []
        if self._package_unload(file_name, track_list=track_list):
            package_record = None
            track_list.reverse()
            log.info("Packages will be reloaded and updated: %s" \
                    % " ".join(
                            map(lambda item: item[0], track_list)
                        )
                )
            for track_item in track_list:
                if track_item[0] in self._packages_loaded:
                    continue
                temp_record = \
                    self._package_load(track_item[0])
                if temp_record is not None:
                    if track_item[0] == file_name:
                        package_record = temp_record
                    log.info("Reloaded and updated package: %s" % file_name)
                else:
                    log.error("Unloaded but could not reload package: %s" \
                            % file_name)
            if not package_record is None:
                return package_record
            else:
                return False
        else:
            log.warning("Could not unload package: %s" % file_name)
        return False

        log.info("Reloaded and updated package: %s" % file_name)

class PackageMessengerThread(Thread):
    """
    PackageMessengerThread does inter-process communication (IPC) to listen
    to commands casted by other processes (potentially from AirWatch, etc.)
    Current implementation of PackageMessengerThread blocks on a named pipe.
    """

    def __init__(self, name=None):
        Thread.__init__(self, name=name)
        self.start()

    def run(self):
        global package_messenger_pipe
        global package_message_queue

        while True:
            with open(package_messenger_pipe, "r") as fp:
                for line in fp.readlines():
                    msg = line.split()
                    if len(msg) > 0:
                        package_message_queue.put(msg)

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
    assert(isinstance(package_path, basestring))
    if os.path.isdir(package_path):
        try:
            os.listdir(package_path)
        except OSError:
            package_path = None
            log.error("Could not access package path")
            return
    else:
        log.debug("Could not find package path: " + package_path)
        try:
            os.makedirs(package_path)
            log.info("Created package path: " + package_path)
        except OSError:
            package_path = None
            log.error("Could not create package path")
            return

    # Validate package messenger pipe
    global package_messenger_pipe
    assert(isinstance(package_messenger_pipe, basestring))
    if os.path.exists(package_messenger_pipe):
        if stat.S_ISFIFO(os.stat(package_messenger_pipe).st_mode):
            pass
        else:
            log.error("Pipe path exists, but it is not a pipe")
            package_messenger_pipe = None
            return
    else:
        package_messenger_pipe_dir = os.path.dirname(package_messenger_pipe)
        if not os.path.isdir(package_messenger_pipe_dir):
            try:
                os.makedirs(package_messenger_pipe_dir)
                log.info("Created directory: " + package_messenger_pipe_dir)
            except OSError:
                package_messenger_pipe = None
                log.error("Could not create directory for messenger pipe")
                return
        try:
            os.mkfifo(package_messenger_pipe)
            log.info("Created pipe: " + package_messenger_pipe)
        except OSError:
            package_messenger_pipe = None
            log.error("Could not create messenger pipe")
            return

    # Will not initialize package manager if package path is mis-configured
    if package_path is None:
        log.error("Package manager failed because package path is invalid")
        return

    # Initialization of package manager
    global package_message_queue
    if package_message_queue is None:
        package_message_queue = Queue()
    global package_lock
    if package_lock is None:
        package_lock = Lock()
    global package_thread
    if package_thread is None:
        package_thread = PackageThread(name="PackageThread")

    # PackageMessengerThread should start last because it triggers actions 
    global package_messenger_thread
    if package_messenger_thread is None:
        if package_thread.isAlive():
            package_messenger_thread = \
                    PackageMessengerThread(name="PackageMessengerThread")
        else:
            log.warning("Package messenger will not start")

    # Mark package manager as initialized
    is_package_manager_initialized = True
    log.info("Package manager is initialized")

# Initialization of this module
if not is_package_manager_initialized:
    initialize()

log.debug("Package manager is imported")
