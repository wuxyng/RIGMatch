# Hybrid Graph Pattern Matching
## Introduction
We develop a novel framework  to address the problem of efficiently finding homomorphic matches for hybrid graph patterns, where each pattern edge may be mapped either to an edge or to a path in the input data, thus allowing for higher expressiveness and flexibility in query formulation.  A key component of our approach is a lightweight index structure called *runtime index graph* (RIG) that leverages graph simulation to compactly encode the query answer search space.  The index can be built on-the-fly during query execution and does not have to  persist on the disk. Using the index, we design a multi-way join algorithm to enumerate query solutions without generating an exploding number of intermediate results. We demonstrate through extensive experiments that our approach can efficiently evaluate a broad spectrum of graph pattern queries and greatly outperforms state-of-the-art approaches.


## Contents

    README ...................  This file
    script/ ..................  Script to run the algorithms
    test/  ...................  Example graphs and queries
    JARs/ ....................  External JARs
    RigMatch/ ................  Sources for our approach


## Requirements

Java JRE v1.8.0 or later

## Input
Both the input query graph and data graph are vertex-labeled. A vertex and an edge are formatted
as 'v VertexID LabelID' and 'e VertexID VertexID EdgeType' respectively. Note that we require that the vertex
id ranges over [0,|V| - 1] where V is the vertex set. The 0 and 1 values of EdgeType denote a child and a descendant edge, respectively. The following is an input sample. You can also find sample data sets and query sets in the test folder.

Example:

```
#q 0
v 0 0 
v 1 1 
v 2 2 
v 3 1 
v 4 2 
e 0 1 0
e 0 2 0
e 1 2 1 
e 1 3 1
e 2 4 0
e 3 4 0
```

## Experiment Datasets

The real world datasets and the corresponding query sets used in our paper as well as cypher queries used for the comparisons with Neo4j can be downloaded from [here](https://drive.google.com/drive/folders/1_-pXpQFY8QvryA5wP6Flk3-bZ-AuUbOs?usp=sharing). Datasets and the corresponding query sets used for comparison with RapidMatch can be downloaded from [there](https://github.com/RapidsAtHKUST/RapidMatch).


