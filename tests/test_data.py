from setcoverage.set import WeightedSet, ExclusionSet

# Include
# ["Glenn", "Jeremy W", "Ben", "Victor", "Vijay"]

# Exclude
# ["Prestinario", "Kamalesh", "Young", "Alex", "Andrea", "Andrew", "Andrey", "Youzhi",
# "Doug", "Daniel", "Eric","Earnest", "Ethan", "Haroon", 'Justin', "Jean", "Jeremy K"]

weighted_sets = [
    WeightedSet(set_id="A10", subset=["Glenn"], weight=100.0),
    WeightedSet(set_id="B20", subset=["Jeremy W", "Ben"], weight=200.0),
    WeightedSet(set_id="C30", subset=["Jeremy W"], weight=300.0),
    WeightedSet(set_id="D40", subset=["Jeremy W", "Glenn", "Ben"], weight=400.0),
    WeightedSet(set_id="E50", subset=["Victor", "Vijay"], weight=500.0),
]
exclusion_sets = [
    ExclusionSet(
        set_id="A10",
        subset_include=["Glenn"],
        subset_exclude=[
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
    ExclusionSet(
        set_id="B20",
        subset_include=["Jeremy W", "Ben"],
        subset_exclude=[
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
    ExclusionSet(
        set_id="C30",
        subset_include=["Jeremy W"],
        subset_exclude=[
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
    ExclusionSet(
        set_id="D40",
        subset_include=["Jeremy W", "Glenn", "Ben"],
        subset_exclude=[
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
    ExclusionSet(
        set_id="E50",
        subset_include=["Victor", "Vijay"],
        subset_exclude=[
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
