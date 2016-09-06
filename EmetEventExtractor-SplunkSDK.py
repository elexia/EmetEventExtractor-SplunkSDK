# -*- coding: utf-8 -*-
import splunklib.client as client
import splunklib.results as results
from BAM import relevance_query, soap_query
from time import clock
from datetime import timedelta
import sys
from time import sleep
import json
import os 


'''
Helper functions: 
get_splunk_events((host, port, username, password, index) - retrieve splunk list, turns it into a dict
get_webreports_events(username, password) - retrieve webreports list, turns it into a dict
submit_events(list_of_dicts, host, port, username, password) - turns dict into a string to submit to splunk
clean_index(host, port, username, password) - clean entire index
'''

class emet_events:
    '''
    Expects a string specifying the Splunk index, Splunk username, 
    password, port, and host, and Webreports username and password.

    A class to hold info of Emet Events from both 
    Splunk and Webreports in  two lists of dictionaries. 
    (splunk_events and webreports_events)
    '''

    # Create an empty list of dicts to hold Splunk and Webreports events 
    splunk_events = []
    webreports_events = []

    # Create Splunk and Webreports credentials
    index = ""
    s_user = ""
    s_pass = ""
    s_port = ""
    s_host = ""
    w_user = ""
    w_pass = ""
    w_server = ""
    w_filter = []
    w_returns = []
    outputpath = ""

    def __init__(self,  wuser, wpass, wserver, wfilter, wreturns, newindex, susername, spassword, shost, sport, output_path):
        
        f.write("<emet_event> : <no parameters> \n")
        # Set all inputs as class variables
        self.w_user = wuser
        self.w_pass = wpass
        self.w_server = wserver
        self.w_filter = wfilter
        self.w_returns = wreturns
        self.index = newindex
        self.s_user = susername
        self.s_pass = spassword
        self.s_host = shost
        self.s_port = sport
        self.outputpath = output_path 

    def get_splunk_events(self, time=""):
        '''
        Retrieves string events from splunk in a list of strings format. 
        Optional argument: timeframe of earliest possible event. Default: no timeframe.
        Expected in one letter format (ex: 1 day = 1d, 2 weeks = 2w, etc.)
        '''

        # Create a Service instance and log in 
        f.write("<get_splunk_events> : <expecting host, port, username, and password to connect to splunk \n")
        service = client.connect(
            host=self.s_host,
            port=self.s_port,
            username=self.s_user,
            password=self.s_pass)

        # Optional : Print installed apps to the console to verify login
        #for app in service.apps:
        #    print app.name

        # If there is a passed argument to time, create timeframe query
        if (time != ""):            
            time = "earliest=-%s" %(time)

        # Set up query and keyword arguments to pull events
        f.write("<get_splunk_events> : <now pulling events from query> \n")
        searchquery_normal = "search index=%s %s" % (self.index, time)
        kwargs_normalsearch = {"exec_mode": "normal", "count" : "0"}
        job = service.jobs.create(searchquery_normal, count=0)

        # Start pulling events (Code from Splunk Python SDK examples)
        # A normal search returns the job's SID right away, so we need to poll for completion
        print "Scanning Splunk index for events... " 
        while True:
            while not job.is_ready():
                pass
            stats = {"isDone": job["isDone"],
                     "doneProgress": float(job["doneProgress"])*100,
                      "scanCount": int(job["scanCount"]),
                      "eventCount": int(job["eventCount"]),
                      "resultCount": int(job["resultCount"])}

            status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                      "%(eventCount)d matched   %(resultCount)d results") % stats

            sys.stdout.write(status)
            sys.stdout.flush()
            if stats["isDone"] == "1":
                sys.stdout.write("\n\nDone!\n\n")
                break
            sleep(2)
        
        # Get the results from the Ordered Dict into a list (Or print them)
        ordered_dictionary = []
        for result in results.ResultsReader(job.results(count=0)):
            #print result
            ordered_dictionary.append(result)
            if (len(ordered_dictionary) % 10 == 0):
                end_time = clock()
                string = "<get_splunk_events> : <%s results have been read in, and %s time has passed. \n>" % (len(ordered_dictionary), timedelta(seconds=end_time - start_time))
                f.write(string)
        
        job.cancel()
    
        # Get the result as a list of strings
        list = []
        for event in ordered_dictionary:
            list.append(event.pop("_raw"))
        
        # Save splunk_events as dictionary
        f.write("<get_splunk_events> : <now saving events into splunk_events and exiting> \n")
        self.splunk_events = self.splunk_parse(list)
        
    def get_webreports_events(self):
        '''
        Pulls recent emet events from webreports based on filter.
        '''

        # Create Emet Return and Search properties
        # Optional EMET Timeframe Settings: Emet Triggered Mitigations for (2 hours, 30 days, a year)
        f.write("<get_webreports_events> : <expecting username and password to webreports> \n")
        search = "computers"

        # Create relevance query and fetch results
        f.write("<get_webreports_events> : <creating query> \n")
        query = relevance_query(self.w_returns, self.w_filter, search)
        f.write("<get_webreports_events> : <getting query results> \n")
        results = soap_query(self.w_user, self.w_pass, self.w_server, query.Query)

        # Save webreports_events as dictionary
        f.write("<get_webreports_events> : <now saving into webreports_events and exiting> \n")
        self.webreports_events = self.webreports_parse(results)
    
    def splunk_parse(self, results):
        '''
        Takes results from a query to splunk, and holds each result in a dictionary,
        then saves them all in a list format to the returned variable "answer".
        Expects a list of results.
        '''
        f.write("<splunk_parse> : <expecting results> \n")

        # Return empty list if there is no result
        if (results == []):
            return []

        # Create needed lists and dictionary for parsing
        answer = []
        list = []
        temp_list = []
        dict = {}

        if(results == []):
            return

        num_keys = results[0].count("=")
        
        # Split with Comma
        f.write("<splunk_parse> : <now parsing string>\n")
        for result in results:
            temp_list.append(result.split(", "))

        # Note : temp_list is now a list of comma separated lists
        # Split results with =
        for count in range(0, len(temp_list)):
            for count2 in range(0, len(temp_list[count])):
                temp_list[count][count2] = temp_list[count][count2].split("=")
                #Append Key
                list.append(temp_list[count][count2][0])
                #Append Value
                list.append(temp_list[count][count2][1])

        # Form a dictionary from the key, value structured list
        count = 0
        for count in xrange(0, len(list), 2):
            count+=1
            dict[list[count]] = list[count+1]
            # If the number of properties have been created into dicts, push complete dict into answer list
            if(count == num_keys):
                answer.append(dict.copy())
                dict.clear()
                count = 0

        f.write("<splunk_parse> : <now returning answer and exiting> \n")
        return answer
    
    def webreports_parse(self, results):
        '''
        Takes results from a query to webreports, and holds each result in a dictionary,
        then saves them all in a list format to the returned variable "answer".
        Expects a list of results.
        ''' 
        
        f.write("<webreports_parse> : <expecting results>\n")
        answer = []
        dict = {}
        
        f.write("<webreports_parse> : <now parsing results> \n")
        # Begin parsing into dictionary form
        for count in range(0, len(results)):
            new_events = results[count]

            # Find number of Emet values by bar count, and start parsing the EMET key/values
            num_properties = new_events.count("|")

            # Remove leading "(" and first comma in EMET TIME value if TIME is a property.
            if (new_events.find("TIME|") != -1):
                new_events = new_events.replace(",", "", 1).replace("( ", "", 1)
            else:
                new_events = new_events.replace("( ", "", 1)
            
            # Split by bars to get EMET key/values
            new_events = new_events.split("|")
            # Remove ending ")" in last emet property
            new_events[num_properties] = new_events[num_properties].replace(" )", "", 1)

            # Note non-emet properties and the final emet property are in the final element
            # of new_events, so we must split them by commas, and save them in a temporary list. 
            # Furthermore, we must clear this incorrect element from new_events.
            temp_list = new_events[num_properties].split(",", len(self.returns)-1)
            new_events.pop(num_properties)

            # Append the non-emet values and final emet value inside of temp_list to new_events
            for event in temp_list: 
                new_events.append(event)
            temp_list = []

            # Remove the ending comma in emet values
            for count in range(1, num_properties): 
                new_events[count] = new_events[count].rsplit(",", 1)

            # Now we have EMET events as list of a string, lists, and another string: 
            # (Assume there are n Key/Value pairs)
            # [ Key1, [Value1, Key2], [Value2, Key3], ...[Value n-1, Key n], Value n ]
            # Normalize into a dictionary of key value pairs

            for count in range(0, num_properties):
                if (count == 0):
                    # First Key/Value Pair ( Key 1, [Value 1, ... ] )
                    dict[new_events[count]] = new_events[count+1][0]
                elif (count == num_properties-1 ):
                    # Last Key/Value Pair ( [ ..., Key n ], Value n ] )
                    dict[new_events[num_properties-1][1]] = new_events[num_properties]
                else:
                    # All Key/Values in the middle ( [ ..., Key ], [ Value, ...] )
                    dict[new_events[count][1]] = new_events[count+1][0]

            # Create dicts out of the non-Emet properties and remove spaces in front of values 
            count = 1
            for return_property in self.returns[1:]:
                dict[return_property.replace(" ","_")] = new_events[num_properties+count][1:]
                count += 1

            # Replace commas in multiple IPs with semicolons to avoid comma parsing confusion 
            if (dict["ip_addresses"].find(")") != -1):
                dict["ip_addresses"] = dict["ip_addresses"].replace(",", ";")

            # Append the dictionary to the return list (answer) and clear the dict for the next event
            answer.append(dict.copy())
            dict.clear()

        end_time = clock()
        string = "<webreports_parse> : <Now returning answer and exiting. %s> \n" % (timedelta(seconds = end_time - start_time))
        f.write(string)
        return answer
        
    def submit_events(self):
        '''
        Turns a dict into a list of strings for submission into splunk index and submits to the given index.
        Automatically updates splunk_events and checks for duplicates in database.
        '''

        f.write("<submit_events> : <expecting host, port, username, and password for splunk> \n")

        # Updating splunk events and checking for duplicates
        self.get_splunk_events()
        self.webreports_events = self.deduplicate()        
        list_of_dicts = self.webreports_events

        # Create a list of events as key=value strings
        f.write("<submit_events> : <now parsing dicts into list of strings> \n")

        list_of_events = []
        temp_string = ""
        for dict in list_of_dicts:
            # Place TIME key/value in the first position
            temp_string += "TIME=%s, " %(dict["TIME"])
            for key in dict:
                if (key != "TIME"):
                    temp_string += "%s=%s, " %(key, dict[key])
            list_of_events.append(temp_string[:-2])
            temp_string = ""

        # Create a Service instance and log in 
        f.write("<submit_events> : <now connecting to client> \n")
        service = client.connect(
            host=self.s_host,
            port=self.s_port,
            username=self.s_user,
            password=self.s_pass)

        # (Optional) Print installed apps to the console to verify login
        #for app in service.apps:
        #    print app.name

        # Get the collection of indexes
        indexes = service.indexes[self.index]

        # Submit into the index 
        f.write("<submit_events> : <now submitting to index and exiting> \n")
        # Only submit characters with accepted characters
        count = 0
        for event in list_of_events:
            event = "".join(i for index in event if ord(index)<128)
            indexes.submit(event, source = "emet", sourcetype = "windows_event_log" )
            count += 1
            if(count % 1000 == 0):
                end_time = clock()
                string = "<submit_events> : <%s events have been submitted and %s has passed.> \n" % (count, timedelta(seconds=end_time - start_time))
                f.write(string)
        
    def clean_index(self):
        '''
        Cleans events out of the given index.
        '''

        # Create a Service instance and log in 
        f.write("<clean_index> : <expecting host, port, username, and password for Splunk> \n")
        service = client.connect(
            host=self.s_host,
            port=self.s_port,
            username=self.s_user,
            password=self.s_pass)

        # Print installed apps to the console to verify login (Optional)
        #for app in service.apps:
        #    print app.name

        # Retrieve the index
        myindex = service.indexes[self.index]

        # Display the current size
        print "Current DB size:", myindex["currentDBSizeMB"], "MB"

        # Clean all events from the index and display its size again
        f.write("<clean_index> : <now cleaning index and exiting> \n")
        timeout = 60
        myindex.clean(timeout)

        print "Current DB size:", myindex["currentDBSizeMB"], "MB"

    def deduplicate(self):
        '''
        Checks for duplicates already in the Splunk index before pushing new events from webreports.
        Returns a list of dicts with unique events to be pushed.
        Expects webreports_events and splunk_events are updated.
        '''
        
        f.write("<deduplicate> : <Creating unique events and submitting into a file.> \n")

        # Deduplicate and order the lists of events
        self.webreports_events = sorted(set(self.webreports_events))
        self.splunk_events = sorted(set(self.splunk_events))

        # Get unique webreports events that are not already in splunk
        unique_events = []
        for event in self.webreports_events:
            if event not in self.splunk_events:
                unique_events.append(event)

        string = "<push_events> : <There are %s unique events that will be pushed to the output file.> \n" % (len(unique_events))
        f.write(string)

        # Push unique events
        splunk_file = open(self.outputpath, "a", 0)
        for event in unique_events:
            splunk_file.write(event)

        end_time = clock() 
        string = "<push_events> : <Exiting function. %s> \n" % (timedelta(seconds = end_time - start_time))
        f.write(string)

start_time = clock()

# Grab commandline argument for config filepath
# Detect Errors if no filepath found or, or if incorrect filepath
try:
    config_file =  sys.argv[1]
    try:
        open(config_file, "r")
    except IOError:
        print "No valid config filepath found."
except IOError:
        print "No config filepath argument found."

# Getting credentials from config file (JSON)
with open(config_file, "r") as data_file:
    data = json.load(data_file)
w_username = data["Settings"][0]["web_username"] 
w_password = data["Settings"][0]["web_password"] 
w_server = data["Settings"][0]["web_server"]
w_returns = data["Settings"][0]["web_returns"]
s_index = data["Settings"][0]["spl_index"]
s_username = data["Settings"][0]["spl_username"]
s_password = data["Settings"][0]["spl_password"]
s_host = data["Settings"][0]["spl_host"]
s_port = data["Settings"][0]["spl_port"]

# Getting filepaths and filters
log_path = data["Settings"][0]["log_path"]
output_path = data["Settings"][0]["output_path"]
w_filter = data["Settings"][1]["web_filters"].values()


# Create Default ouput and log paths
if (log_path == ""):
    local_path = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(local_path, "EMETEventExtractor_Logs.txt")
if (output_path == ""):
    local_path = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(local_path, "Splunk_Events.txt")

# Clearing and opening Logs
f = open(log_path, "w")
f = open(log_path, "a", 0)

##TESTING##
save = emet_events(w_username, w_password, w_server, w_filter, w_returns, s_index, s_username, s_password, s_host, s_port, output_path)
#save.clean_index()
#save.get_webreports_events("30 days")
#save.submit_events()
#save.get_splunk_events()

##FINISHING##
end_time = clock()
string = "The program took %s." % (timedelta(seconds=end_time - start_time))
f.write(string)