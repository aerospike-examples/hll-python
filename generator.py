import random

LOCATION = [
    ["vancouver", "canada"],
    ["vancouver", "washington"],
    ["mountain_view", "california"],
    ["seattle", "washington"],
    ["dublin", "ireland"],
    ["dublin", "california"],
]

TAGS = [
    # search terms
    ["iceland", "elves", "waterfalls"],
    ["hiking", "camping", "waterfalls"],
    ["hobbits", "elves", "fiction", "fantasy"],
    ["tesla", "mercedes", "bmw", "toyota"],
    ["tesla", "faraday", "newton"],
    ["database", "aerospike"],
    ["rocket", "aerospike"],
    # hobbies
    ["tennis"],
    ["kniting"],
    ["baking"],
    ["fishing"],
    # owned cars
    ["tesla"],
    ["mercedes"],
    ["bmw"],
    ["toyota"],
]


def get_index(fraction: float, distribution: list) -> int:
    total = sum(distribution)
    part = fraction * total
    index = 0

    for d in distribution:
        if part < d:
            return index
        index += 1
        part -= d

    return index


def get_location() -> str:
    return LOCATION[get_index(random.random(), [7, 1, 6, 8, 3, 2])]


def get_tags() -> list:
    samples = 1 + int(random.expovariate(1.5))
    tags = random.sample(TAGS, samples)

    # flatten nested lists and remove duplicates
    return list({j for i in tags for j in i})


def get_profile(id: int, time_quantum: int) -> list:
    profile = []
    random.seed(id)
    profile.extend(get_location())
    random.seed(id * 1000000 + time_quantum)
    profile.extend(get_tags())
    return profile
