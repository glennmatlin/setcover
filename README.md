pyspark-setcover
================

## Summary
Package to solve set coverage problems using a greedy algorithm to approximate the optimal solution.

Algorithm picks set with the lost set cost (minimization constraint) compared to the number of new elements covered (maximization).

Project goal is to implement solver for use on distributed systems with PySpark 3, Dask, etc.


## Explained
```
Complexity: U * log(S)

Universe U of n elements

Subsets S of U:
    S = (S1, S2, ..., Sm) Where every subset Si has an associated cost.

Find a minimum weight subcollection of S that covers all elements of U

Example:

    U = {1,2,3,4,5}
    
    S = {S1,S2,S3}

    S1, Cost(S1) = ({4,1,3}, 5)

    S2, Cost(S2) = ({2,5}, 10)

    S3, Cost(S3) = ({1,4,3,2}, 3)

Output:

    SetCover(U,S) == ({S2, S3}, 13)
```

## Requirements

miniconda3

## Install

```shell
conda config --add channels conda-forge 
conda config --set channel_priority strict 
conda create -n pyspark-setcover -f environment.yml
```

## Testing
 
Two methods:

1. Makefile: `make`
2. Manual: `python3 -m pytest tests` from root

## References:

### Method:
- https://en.wikipedia.org/wiki/Set_cover_problem
- http://www.cs.ucr.edu/~neal/Young08SetCover.pdf
- https://www.youtube.com/watch?v=MEz1J9wY2iM
- https://www.youtube.com/watch?v=cjSeHSjPmsk&t=195s

### Code:
- https://github.com/guangtunbenzhu/SetCoverPy
- https://github.com/TheAlgorithms/Python
- https://github.com/suzhiyang/weightedsetcover
- https://github.com/jwg4/exact_cover
- https://github.com/Oovvuu/weightedset
