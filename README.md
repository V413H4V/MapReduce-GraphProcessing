# MapReduce - Grouping Nodes based on number of neighbors
=================================================================================
### This project implements a simple graph algorithm that needs two Map-Reduce jobs. A directed graph is represented as a text file where each line represents a graph edge. 

### For example,
20,40
represents the directed edge from node 20 to node 40. 

### First, for each graph node, we compute the number of node neighbors. 
### Then, we group the nodes by their number of neighbors and for each group we count how many nodes belong to this group. 

### The result will have lines such as:
10 30
which says that there are 30 nodes that have 10 neighbors.
