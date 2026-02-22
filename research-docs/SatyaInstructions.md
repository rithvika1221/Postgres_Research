# This file contains the weekly instructions from my mentor every week. 
# I use this to decide on what are the next steps to do.

# 
Next steps:

In the next sync up, we can define the workload exactly. Get an Azure subscription, and run the same experiment on large machines with high concurrency. Be frugal in provisioning, and plan to complete the run in a few hours. May be a 8 vcore machine is good, Run the workload with 1,4,8,16,32, and 64 users.

Artifacts from your experiments:

1/ GitHub repository with your automated scripts and a read me file on the steps to follow including environment setup etc.
	Explain the metrics you collected, why they are important, for example, lag, WAL generation rate, tps
	Checkin the automation scripts
2/ Perf run report
	Explain the motivation
	How postgres logical replication works
	Workload design
		What workloads you tested and why they are relvant
		What threads and concurrency you used
		Hardware info if any
	Results
		Throughput vs lag with varying concurrency
		Stead state behavior
	Analysis
		Explain why lag increases, and what point it does
		Present cpu and memory changes
	Future work:
		For example compare physical vs logical replication
		Trying other workloads etc.

3/ Also write this report in medium or LinkedIn post