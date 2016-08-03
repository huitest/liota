#!/bin/bash

liota_config="/etc/liota/conf/liota.conf"
package_messenger_pipe=""

if [ ! -f "$liota_config" ]; then
	echo "ERROR: Configuration file not found" >&2
	exit -1
fi

while read line # Read configurations from file
do
	if echo $line | grep -F = &>/dev/null
	then
		varname=$(echo "$line" | sed "s/^\(..*\)\s*\=\s*..*$/\1/")
		if [ $varname == "pkg_msg_pipe" ]; then
	    	value=$(echo "$line" | sed "s/^..*\s*\=\s*\(..*\)$/\1/")
			package_messenger_pipe=$value
		fi
	fi
done < $liota_config

if [ "$package_messenger_pipe" == "" ]; then
	echo "ERROR: Pipe path not found in configuration file" >&2
	exit -2
fi

if [ ! -p "$package_messenger_pipe" ]; then
	echo "ERROR: Pipe path is not a named pipe" >&2
	exit -3
fi

# Echo to named pipe
echo "Pipe file: $package_messenger_pipe" >&2
echo "$@" > $package_messenger_pipe

