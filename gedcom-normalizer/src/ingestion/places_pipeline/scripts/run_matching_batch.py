df = pd.read_csv("places_normalized.csv")

matcher = GeoMatcher(...)

results = []

for row in df.itertuples():

    result = matcher.match(
        row.place_clean,
        parts=[
            row.place_part1,
            row.place_part2,
            row.place_part3,
            row.place_part4,
            row.place_part5,
            row.place_part6,
        ]
    )

    results.append(result)

pd.DataFrame(results).to_csv(...)