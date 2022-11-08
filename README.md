# Cypher, Datalog Playground

This repository serves as a playground for a comparison of two query languages that 
are suited for querying deeply connected data (cypher & datalog).

... it is also a good example through which I am getting familiar with Clojure :) 

## Run

We must first have a running instance of the Neo4j graph database against which we will
run cypher queries. 
```
docker run -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/s3cr3t neo4j
```
XTDB is the other database (for datalog queries), but it runs in memory.

Now all that is left to do is run `lein run` which will run the program.
The program does the following (the same for both cypher & datalog):
- Imports a subset of the [Northwind dataset](https://docs.yugabyte.com/preview/sample-data/northwind/) into the database.
- Prints out queries, executes them and prints out their results.


