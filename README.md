# DistributedSystems
Labs from following 6.824: Distributed Systems (https://pdos.csail.mit.edu/6.824/index.html)

## Lab 1: Map-Reduce

The objective of lab 1 is to create a MapReduce system in Golang. A Master process and several Worker processes are ran through the command line and communicate through RPCs. The Master keeps record of the amount of tasks required (int this case it is the number of word documents to be parsed) and the workers request a task to either Map (count the frequency of each word in the document) or Reduce (tally the overall frequency of a word across all word documents).

