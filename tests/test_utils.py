from setcover.utils import flatten_list, flatten_set

nested_list = [
    ["Glenn", "Vijay"],
    ["Jeremy W", "Ben"],
    ["Jeremy W", "Victor"],
    [],
    ["Victor", "Glenn", "Vijay"],
]


class TestUtils:
    def test_flatten_list(self):
        assert flatten_list(nested_list) == [
            "Glenn",
            "Vijay",
            "Jeremy W",
            "Ben",
            "Jeremy W",
            "Victor",
            "Victor",
            "Glenn",
            "Vijay",
        ]

    def test_flatten_set(self):
        assert flatten_set(nested_list) == {
            "Vijay",
            "Glenn",
            "Ben",
            "Jeremy W",
            "Victor",
        }
