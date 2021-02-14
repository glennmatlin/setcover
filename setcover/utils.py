from typing import List, Iterable, Set


def flatten_list(nested_list: List[object]) -> List[object]:
    flat_list = [item for sublist in nested_list for item in sublist]
    return flat_list


def flatten_set(nested_iterable: Iterable[object]) -> Set[object]:
    flat_set = set(flatten_list(nested_iterable))
    return flat_set
