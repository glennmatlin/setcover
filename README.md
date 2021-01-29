Weighted Set Cover
================

## Summary
Solution for the "Weighted Set Cover Problem" using a greedy algorithm which approximates the optimal solution.

Algorithm picks set with the lowest ratio of set weight to the number of new elements covered.

```
Universe U of n elements Collection of subsets of U:

    S = S1,S2...,Sm Where every substet Si has an associated cost.

Find a minimum cost subcollection of S that covers all elements of U

Example:

    U = {1,2,3,4,5} S = {S1,S2,S3}

    S1 = {4,1,3}, Cost(S1) = 5 S2 = {2,5}, Cost(S2) = 10 S3 = {1,4,3,2}, Cost(S3) = 3

    Output:
        Set cover = {S2, S3} Min Cost = 13
```

Time complexity of algorithm in BigO is `O(|U| * log|S|)`.

## References:

### Reading:
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
