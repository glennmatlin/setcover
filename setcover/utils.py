import numpy as np
from typing import Iterable, List, Set, Dict


def flatten_nest(nest: Iterable[Iterable[object]], output="set") -> Iterable[object]:
    if output == "set":
        return set([item for sublist in nest for item in sublist])
    elif output == "list":
        return [item for sublist in nest for item in sublist]
    else:
        raise TypeError("Output must be 'set' or 'list'")


def get_from_map(map, stuff):
    return [map[thing] for thing in stuff]


def get_tokens_idx(token_map, tokens):
    return [token_map[token] for token in tokens]


def get_idxs_token(idx_map, idxs):
    return [idx_map[idx] for idx in idxs]


def token_to_idx(tokens: List[str], token_idx_map: Dict[str, int]):
    """assert token_to_idx(["G", "I"], {"G": 1, "H": 2, "I": 3}) == [1, 3]"""
    output = []
    for token in tokens:
        output.append(token_idx_map[token])
    return output


def reverse_dictionary(dictionary: Dict[str, int]):
    """assert reverse_dictionary(dictionary={"a": 1, "b": 2}) == {1: "a", 2: "b"}"""
    reversed_dictionary = {value: key for (key, value) in dictionary.items()}
    return reversed_dictionary


def make_subset_array(Row, n_total, id_field="set_id", token_field="set_tokens"):
    # TODO Make function more generic
    output = (Row[id_field], np.zeros(n_total))
    dx_positive_idxs = get_tokens_idx(Row[token_field])
    output[1][dx_positive_idxs] = 1.0
    return output
