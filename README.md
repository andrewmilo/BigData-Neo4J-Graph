Using the NoSQL graph database, Neo4J, we were able to create relationships between users, their skills/interests, and organizations.
Afterwards, we were able to hand craft a few sample queries to make life easier.

### Technology Used
- Neo4J
- Python

### Preview of a subset (graph was too big)
![alt text](OurGraph.png)

### UML Diagram
![alt text](CollaboratorDesignDiagram.png)

### Potential Improvements
1. When we use PERIODIC COMMIT to commit queries in stacks of 10000, we do so in an attempt to prevent any out of memory exceptions. 
It is successful in doing so in our tests, however there is still a possibility of a memory error due to Eager Loading. Eager Loading 
is when a query for one entity type also loads related entities as part of the query. It is sometimes valuable to circumvent eager loading 
through the manipulation of queries, such as splitting a query into multiple parts and isolating changes in queries that affect any MATCH/MERGE 
statements that follow.

2. Indexing could possibly be used more to allow for a faster and more efficient search when using MATCH or MERGE. However, it can also be 
counter-productive to index too many nodes or relationships, as it may eventually slow down querying. We chose to keep it to only User and 
Organization since they were the most commonly referenced nodes. However, adding an Interest index could have increased the query speeds
for this particular project.

3. Increasing the amount of memory allocated to Neo4j could be beneficial in speeding up queries. This goes hand in hand with the Cache Type
implementation in Neo4j. There are 5 Cache Types -- none, soft, weak, strong, hpc. Depending on how big the graph is, the nodes and relationships 
can be loaded into memory for varying amounts of time. For example, if the graph is small, then in a Strong type cache the graph can be loaded 
entirely into the cache and better performance could be achieved. We kept the default of the soft type cache, which provides an optimal balance
between performance and memory management. The HPC cache type might be best for large graphs, as it assigns dedicated memory for nodes and 
relationships which has a small footprint.
