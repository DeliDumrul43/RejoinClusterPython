import subprocess
import json
import time
from datetime import datetime

def log(message):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] {}".format(current_time, message))

password = 'passwd'

# Run mysqlsh command to get cluster status
mysqlsh_command = "mysqlsh --uri someCluster@some-hadoop-02:3306 --password=somepasswd! -e \"dba.getCluster('someCluster').status()\" -i"
process = subprocess.run(mysqlsh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

log("Stdout:\n{}".format(process.stdout))
log("Stderr:\n{}".format(process.stderr))

checkClusterErrorState = process.stderr.find("ERROR")

if checkClusterErrorState != -1:
    log("Couldn't check the status, checking on a different cluster")
    mysqlsh_command = "mysqlsh --uri someCluster@some-hadoop-04:3306 --password=somepasswd! -e \"dba.getCluster('someCluster').status()\" -i"
    process = subprocess.run(mysqlsh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

# Check if the command was successful or not; if not, exit() to handle potential issues
if process.returncode != 0:
    log("Error executing mysqlsh command: {}".format(process.stderr))
    exit()

# Extract JSON part from process.stdout, take '{' as the starting point
start_position = process.stdout.find('{')
json_data = json.loads(process.stdout[start_position:])

# Rest of the script remains unchanged ---- same as bash one
# Extract server names with status "OFFLINE"
online_servers = [server for server, data in json_data['defaultReplicaSet']['topology'].items() if data['status'] == '(MISSING)']

if not online_servers:
    log("There is no offline cluster, but please check the logs for guarantee")

# Iterate through the online servers and rejoin the addresses ---- 
for server in online_servers:
    address = json_data['defaultReplicaSet']['topology'][server]['address']
    log("Server is OFFLINE, Address: {}".format(address))

    cluster_uri = "oredataCluster@{}".format(address)
    log(cluster_uri)

    rejoin_command = (
        "mysqlsh --uri someCluster@some-hadoop-05:3306 --password=somepasswd -e "
        "\"shell.connect('some@some-hadoop-05:3306', 'somepasswd'); "
        "var c = dba.getCluster('someCluster'); "
        "c.rejoinInstance('{}', {{ password: 'somepasswd' }});\" -i".format(cluster_uri)
    )

    output_rejoin_action = subprocess.run(rejoin_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    log("Stdout: {}".format(output_rejoin_action.stdout))
    log("Stderr: {}".format(output_rejoin_action.stderr))

    isClusterErrorState = output_rejoin_action.stderr.find("ERROR")
    if isClusterErrorState != -1:
        log("Rejoin failed because the cluster that tried to rejoin the process was missing. Now trying on another cluster")

        rejoin_command_2 = (
        "mysqlsh --uri someCluster@some-hadoop-04:3306 --password=somepasswd -e "
        "\"shell.connect('oredataCluster@some-hadoop-04:3306', 'somepasswd'); "
        "var c = dba.getCluster('someCluster'); "
        "c.rejoinInstance('{}', {{ password: 'somepasswd' }});\" -i".format(cluster_uri)
        )

        output_rejoin_action_2 = subprocess.run(rejoin_command_2, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        log("Stdout: {}".format(output_rejoin_action_2.stdout))
        log("Stderr: {}".format(output_rejoin_action_2.stderr))
    else:
        log("There is no problem with the rejoin process")



