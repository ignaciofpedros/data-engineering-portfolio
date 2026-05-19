def fallback_score(place, candidate):

    return fuzz.ratio(place, candidate["full_path"])