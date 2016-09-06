# EmetEventExtractor-SplunkSDK
An alternate version of EmetEventExtractor which makes use of the Splunk Python SDK instead of a file cache.

This script retrieves EMET triggered mitigations from the BigFix Webreports Database using BAM functions and from Splunk using the Splunk Python SDK. The Webreports events are then deduplicated and checked against the Splunk retreived events. The script submits new, unique files directly to the Splunk index using the SDK.

##Cool Features

Configuration Files

This script depends on a config file formatted in JSON. Customizable settings include: Username, Password, Server, Filters, Returns, Log Filepath, and Output Filepath.

####Logs

The script also documents its activity using logs. It will create a file and update the process of the program in each function. This should help out with debugging. 

####Parsing

The script will take a list of strings and parse them into key-valued dictionaries to make them easier to read for Splunk. Each event will be submitted in "key"="value" order.

####Try/Except

This script uses the try/except pairing with common errors so as not to stop automated scripts from running. Some common errors such as unicode encoding are ignored and documented in the logs.
