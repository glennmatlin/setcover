from src.weightedsetcover import weightedsetcover

# TODO Get test cases from existing dataset

# data = {
#     'A10': {'patients': {'Glenn', 'Jeremy', 'Ben'}, 'weight':1000},
#     'B20': {'patients': {'Jeremy'}, 'weight': 20},
#     'C30': {'patients': {'Glenn'}, 'weight': 30},
#     'D40': {'patients': {'Glenn', 'Ben'}, 'weight': 40},
#     'E50': {'patients': {'Ben', 'Jeremy'}, 'weight': 50},
# }

# {'code': {0: 'A0100', 1: 'A020', 2: 'A028', 3: 'A029', 4: 'A030'},
#  'patient_ids': {0: ['ImwKm7mel9wAhH9HV3HYny1nJD6vvzGLBOy/wctFNkA='],
#   1: ['HYEm8QH+Kf6EeAw0GKUyfsMd26EADF64+P3wM+Nw4AQ=',
#    'GFCZeovKt3sH95oZDsHqtoEbk+1mpVSJbRdSmJXNhuo='],
#   2: ['HYEm8QH+Kf6EeAw0GKUyfsMd26EADF64+P3wM+Nw4AQ='],
#   3: ['HYEm8QH+Kf6EeAw0GKUyfsMd26EADF64+P3wM+Nw4AQ=',
#    'GFCZeovKt3sH95oZDsHqtoEbk+1mpVSJbRdSmJXNhuo='],
#   4: ['iG9yboiqP8gjbSsq/XYW6hEvZnzuUqGjlI/XjEuiqkw=',
#    'wTZqXY2//Ja1Zj/lZTkZS+2ReS37zB/0z54t//w/2FY=']},
#  'patient_count': {0: 161.0, 1: 782.0, 2: 56.0, 3: 310.0, 4: 36.0}}

# TODO sets should be ICD[Patient,Patient] since we want to find the overlapping ICD codes
# TODO create a named tuple object to contain the ICD/Patients/Weight

sets = [
    [1, 2, 3],
    [3, 6, 7, 10],
    [8],
    [9, 5],
    [4, 5, 6, 7, 8],
    [4, 5, 9, 10],
]
weights = [1, 2, 3, 4, 3, 5]


def test_weightedsetcover():
    selected, cost = weightedsetcover(sets, weights)
    assert (selected, cost) == ([0, 4, 1, 3], 10)