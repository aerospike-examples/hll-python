import random

LOCATION = [
    [
        "vancouver",
        "canada",
    ],
    [
        "vancouver",
        "washington",
    ],
    [
        "mountain_view",
        "california",
    ],
    [
        "seattle",
        "washington",
    ],
    [
        "dublin",
        "ireland",
    ],
    [
        "dublin",
        "california",
    ],
]

TAGS = [
    # search terms
    [
        "iceland",
        "elves",
        "waterfalls",
    ],
    [
        "hiking",
        "camping",
        "waterfalls",
    ],
    [
        "hobbits",
        "elves",
        "fiction",
        "fantasy",
    ],
    [
        "tesla",
        "mercedes",
        "bmw",
        "toyota",
    ],
    [
        "tesla",
        "faraday",
        "newton",
    ],
    [
        "database",
        "aerospike",
    ],
    [
        "rocket",
        "aerospike",
    ],
    # hobbies
    [
        "tennis",
    ],
    [
        "kniting",
    ],
    [
        "baking",
    ],
    [
        "fishing",
    ],
    # owned cars
    [
        "tesla",
    ],
    [
        "mercedes",
    ],
    [
        "bmw",
    ],
    [
        "toyota",
    ],
]


def get_index(fraction, distribution):
    total = 0
    for d in distribution:
        total += d
    part = fraction * total
    index = 0
    for d in distribution:
        if part < d:
            return index
        index += 1
        part -= d
    return index


def get_location(rnd):
    return LOCATION[get_index(rnd.random(), [7, 1, 6, 8, 3, 2])]


def get_tags(rnd):
    samples = 1 + int(rnd.expovariate(1.5))
    tags = rnd.sample(TAGS, samples)

    # flatten nested lists
    tags = [j for i in tags for j in i]

    # remove duplicates
    return list(set(tags))


def get_profile(id, time_quantum):
    profile = []
    random.seed(id)
    profile.extend(get_location(random))
    random.seed(id * 1000000 + time_quantum)
    profile.extend(get_tags(random))
    return profile
