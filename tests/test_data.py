from src.set import WeightedSet, ExclusionSet

# Include
# ["Glenn", "Jeremy W", "Ben", "Victor", "Vijay"]

# Exclude
# ["Prestinario", "Kamalesh", "Young", "Alex", "Andrea", "Andrew", "Andrey", "Youzhi",
# "Doug", "Daniel", "Eric","Earnest", "Ethan", "Haroon", 'Justin', "Jean", "Jeremy K"]
weighted_data = [
    ("A10", ["Glenn", "Vijay"], 100.0),
    ("B20", ["Jeremy W", "Ben"], 200.0),
    ("C30", ["Jeremy W", "Victor"], 300.0),
    ("D40", ["Glenn", "Ben"], 400.0),
    ("E50", ["Victor", "Glenn", "Vijay"], 500.0),
]
weighted_sets = [
    WeightedSet(set_id=w[0], subset=w[1], weight=w[2]) for w in weighted_data
]
exclusion_data = [
    (
        "A10",
        ["Glenn", "Vijay"],
        [
            "Jean",
            "Daniel",
            "Youzhi",
            "Prestinario",
            "Young",
            "Haroon",
            "Andrea",
            "Eric",
            "Kamalesh",
            "Jeremy K",
            "Ethan",
        ],
    ),
    (
        "B20",
        ["Jeremy W", "Ben"],
        [
            "Earnest",
            "Daniel",
            "Andrew",
            "Alex",
            "Prestinario",
            "Justin",
            "Kamalesh",
            "Jean",
            "Haroon",
            "Andrea",
            "Doug",
            "Andrey",
            "Young",
            "Jeremy K",
            "Youzhi",
        ],
    ),
    (
        "C30",
        ["Jeremy W", "Victor"],
        [
            "Andrey",
            "Haroon",
            "Eric",
            "Jeremy K",
            "Andrea",
            "Earnest",
            "Alex",
            "Doug",
            "Young",
            "Kamalesh",
            "Andrew",
            "Prestinario",
            "Youzhi",
        ],
    ),
    (
        "D40",
        ["Glenn", "Ben"],
        [
            "Andrea",
            "Andrey",
            "Kamalesh",
            "Andrew",
            "Alex",
            "Youzhi",
            "Justin",
            "Earnest",
            "Prestinario",
            "Young",
            "Haroon",
            "Daniel",
            "Jean",
            "Eric",
            "Doug",
            "Ethan",
        ],
    ),
    (
        "E50",
        ["Victor", "Glenn", "Vijay"],
        [
            "Jeremy K",
            "Kamalesh",
            "Andrew",
            "Prestinario",
            "Daniel",
            "Ethan",
            "Jean",
            "Haroon",
            "Eric",
            "Justin",
            "Doug",
            "Young",
            "Alex",
        ],
    ),
]

exclusion_sets = [
    ExclusionSet(set_id=w[0], subset_include=w[1], subset_exclude=w[2]) for w in exclusion_data
]