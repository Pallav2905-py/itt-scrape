#!/bin/bash

# Run Python script in background with nohup
# This will keep running even after SSH disconnect

nohup python collect_gmail.py > collect_gmail.log 2>&1 &

# Save the process ID
echo $! > collect_gmail.pid

echo "Script started in background with PID: $(cat collect_gmail.pid)"
echo "Logs: tail -f collect_gmail.log"
echo "Stop: kill \$(cat collect_gmail.pid)"