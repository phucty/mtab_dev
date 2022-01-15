import jellyfish as jf
from rapidfuzz import fuzz, process


def sim_string_fuzz(a, b, w1=1, w2=1, w3=1, w4=1, is_lower=True):
    sim_all = 0
    if a and b and len(a) and len(b):
        if is_lower:
            a = a.lower()
            b = b.lower()
        sim1 = fuzz.partial_ratio(a, b)
        sim2 = fuzz.ratio(a, b)
        sim3 = fuzz.token_sort_ratio(a, b)
        sim4 = fuzz.token_set_ratio(a, b)
        sim_all = (
            (sim1 * w1 + sim2 * w2 + sim3 * w3 + sim4 * w4) / (w1 + w2 + w3 + w4) / 100
        )
        # sim_all = (sim1 * w1 + sim2 * w2) / (w1 + w2) / 100.
        # sim_all = (sim1+sim2+sim3+sim4) / 4. / 100.
    return sim_all


def dis_func(func, str_1, str_2, is_lower=False):
    """Calculate distances

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.
        func ([type], optional): distance function. Defaults to jf.damerau_levenshtein_distance.

    Returns:
        float: distance
    """
    result = 0
    if str_1 and str_2 and len(str_1) and len(str_2):
        if is_lower:
            str_1 = str_1.lower()
            str_2 = str_2.lower()
        result = float(func(str_1, str_2))
    return result


def dis_daumerau_levenshtein(str_1, str_2, is_lower=False):
    """Calculate Daumerau Levenshtein distance

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Daumerau Levenshtein distance
    """
    return dis_func(jf.damerau_levenshtein_distance, str_1, str_2, is_lower=is_lower)


def dis_levenshtein(str_1, str_2, is_lower=False):
    """Calculate Levenshtein distance

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Levenshtein distance
    """
    return dis_func(jf.levenshtein_distance, str_1, str_2, is_lower=is_lower)


def dis_hamming(str_1, str_2, is_lower=False):
    """Calculate Hamming distance

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Hamming distance
    """
    return dis_func(jf.hamming_distance, str_1, str_2, is_lower=is_lower)


def sim_jaro(str_1, str_2, is_lower=False):
    """Calculate Jaro similarity

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Jaro similarity, 0 means dissimilar, 1 means similar
    """
    return dis_func(jf.jaro_distance, str_1, str_2, is_lower=is_lower)


def sim_jaro_winkler(str_1, str_2, is_lower=False):
    """Calculate Jaro Winkler similarity

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Jaro similarity, 0 means dissimilar, 1 means similar
    """
    return dis_func(jf.jaro_winkler, str_1, str_2, is_lower=is_lower)


def sim_fuzz_ratio(str_1, str_2, is_lower=False):
    """Calculate Fuzz Levenshtein similarity

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Fuzz Levenshtein similarity, 0 means dissimilar, 1 means similar
    """
    return dis_func(fuzz.ratio, str_1, str_2, is_lower=is_lower) / 100.0


def sim_fuzz_partial_ratio(str_1, str_2, is_lower=False):
    """Calculate Fuzz Partial Levenshtein similarity

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Fuzz Partial Levenshtein similarity, 0 means dissimilar, 1 means similar
    """
    return dis_func(fuzz.partial_ratio, str_1, str_2, is_lower=is_lower) / 100.0


def sim_fuzz_token_sort_ratio(str_1, str_2, is_lower=False):
    """Calculate Fuzz Token Sorted Levenshtein similarity

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Fuzz Token Sorted Levenshtein similarity, 0 means dissimilar, 1 means similar
    """
    return dis_func(fuzz.token_sort_ratio, str_1, str_2, is_lower=is_lower) / 100.0


def sim_fuzz_token_set_ratio(str_1, str_2, is_lower=False):
    """Calculate Fuzz Token Set Levenshtein similarity

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.

    Returns:
        float: Fuzz Token Set Levenshtein similarity, 0 means dissimilar, 1 means similar
    """
    return dis_func(fuzz.token_set_ratio, str_1, str_2, is_lower=is_lower) / 100.0


def sim_fuzz(str_1, str_2, is_lower=False, w1=1, w2=1, w3=1, w4=1):
    """Calculate fuzzywuzzy

    Args:
        str_1 (str): string 1
        str_2 (str): string 2
        is_lower (bool, optional): lower two strings. Defaults to False.
        w1 (int, optional): ratio weight. Defaults to 1.
        w2 (int, optional): partial ratio weight. Defaults to 1.
        w3 (int, optional): token sort ratio weight. Defaults to 1.
        w4 (int, optional): token set ratio weight. Defaults to 1.

    Returns:
        float: [0-1], 0 means dissimilar, 1 means similar
    """
    result = 0
    if str_1 and str_2 and len(str_1) and len(str_2):
        if is_lower:
            str_1 = str_1.lower()
            str_2 = str_2.lower()
        sim1 = fuzz.partial_ratio(str_1, str_2)
        sim2 = fuzz.ratio(str_1, str_2)
        sim3 = fuzz.token_sort_ratio(str_1, str_2)
        sim4 = fuzz.token_set_ratio(str_1, str_2)
        result = (
            (sim1 * w1 + sim2 * w2 + sim3 * w3 + sim4 * w4) / (w1 + w2 + w3 + w4) / 100
        )
    return result


def get_closest(str_1, str_list):
    respond = process.extractOne(str_1, str_list, scorer=fuzz.ratio)
    if respond:
        return respond[0], respond[1] / 100
    else:
        return None


def get_closest_1(func, str_1, str_list):
    """Get the most relevant item to str_1 from the list of string

    Args:
        func (function): similarity or distance function
        str_1 (str): string 1
        str_list (list): list of compared strings

    Returns:
        str: item
        float: score
    """
    is_sim = True if "sim_" in func.__name__ else False
    closet_score = -1.0 if is_sim else 100.0
    closet_item = None
    if str_1 and len(str_1) and str_list and len(str_list):
        str_list = sorted(str_list, key=lambda x: abs(len(x) - len(str_1)))
        for str_i in str_list:
            tmp = func(str_1, str_i, is_lower=False)
            if is_sim and tmp > closet_score:
                closet_score = tmp
                closet_item = str_i
                if closet_score >= 1.0:
                    break
            if not is_sim and tmp < closet_score:
                closet_score = tmp
                closet_item = str_i
                if closet_score == 0:
                    break
    return closet_item, closet_score


def sim_percentage_change(obj_1, obj_2):
    """Calculate the percentage change of two objects

    Args:
        obj_1 ([type]): object 1
        obj_2 ([type]): object 2

    Returns:
        float: numerical similarity
    """
    try:
        a = float(obj_1)
        b = float(obj_2)
        max_ab = max(abs(a), abs(b))
        if max_ab == 0:
            if a == b:
                return 1
            if a != b:
                return 0
        return 1 - abs(a - b) / max_ab
    except ValueError:
        return 0


def test_sim_num():
    """Test numerical similarities"""
    pairs = [["9", 100], ["nine", 100]]
    for str_1, str_2 in pairs:
        print(
            f"{sim_percentage_change(str_1, str_2):.2f}: {str(str_1):s} - {str(str_2):s}"
        )


def test_dis_text():
    """Test textual distances"""
    pairs = [
        ["jellyifhs", "jellyfish"],
        ["ifhs", "fish"],
        ["fuzzy wuzzy was a bear", "wuzzy fuzzy was a bear"],
    ]
    funcs = [
        dis_levenshtein,
        dis_daumerau_levenshtein,
        dis_hamming,
        sim_jaro,
        sim_jaro_winkler,
        sim_fuzz,
    ]

    for str_1, str_2 in pairs:
        print(f"\n{str_1:s} - {str_2:s}")
        for func in funcs:
            print(f"{func(str_1, str_2):.2f}: {func.__name__:s}")


def test_closest():
    # str_1 = "new york jets"
    # choices = ["Atlanta Falcons", "New York Jets", "New York Giants", "Dallas Cowboys"]

    choices = ["Sarah McLachlan"]
    str_1_list = [
        "Sarrah Mcgloclyn",
        "Sarrah Mcloklinn",
        "SSarah  Macklochlin",
        "SSarah Machaclann",
        "Saraah Mcglaklin",
        "Sarrah  Mclaclahan",
        "Saraah MMcclahlen",
        "Sarrah Mcglauklen",
        "Saraah Mcccloghlin",
        "Sarrah Mcgloucklann",
        "Sarraah Mclaukyn",
        "Sarahh Maclalahann",
        "SSarah Maglaphlin",
        "SSarah Mcclaughcllan",
        "Saraah Mcchlachann",
        "Saarah Mcgluacliin",
        "SSarah Maccllauchan",
        "Saraah Mclohlhann",
        "Saarah Maclaclaunn",
        "Saarah Mughlochlan",
        "Sarrah Mcgoclian",
        "Sarahh Mclafdja",
        "SSarah Mcglachelenn",
        "Sarrah Mcgglothan",
        "Saraah Mclaklpeen",
        "Sarrah Mcgloghlaiin",
        "Saraah Magluchalan",
        "Sarrah Meglloghlan",
        "SSarah Mcglocnan",
        "SSarah Mcluklinn",
        "Sarah Mcglocchilann",
        "Sarrah Mcglclainn",
        "SSarah Mcgloccklam",
        "Sarrah Mgclocklinn",
        "Sarahh Mcglaculainn",
        "SSarah MMclaquelin",
        "SSarah Maglauhlinn",
        "Saraah Mcglauhignn",
    ]
    funcs = [
        dis_levenshtein,
        dis_daumerau_levenshtein,
        dis_hamming,
        sim_jaro,
        sim_jaro_winkler,
        sim_fuzz,
        sim_fuzz_ratio,
        sim_fuzz_partial_ratio,
        sim_fuzz_token_sort_ratio,
        sim_fuzz_token_set_ratio,
    ]

    for str_1 in str_1_list:
        print(f"\n{str_1:s}")
        for func in funcs:
            c_i, c_s = get_closest(func, str_1, choices)
            print(f"{c_s:.2f}[{c_i:s}]: {func.__name__:s}")


def test_sim():
    query = "Sarah McLachlan"
    str_1_list = [
        "Sarrah Mcgloclyn",
        "Sarrah Mcloklinn",
        "SSarah  Macklochlin",
        "SSarah Machaclann",
        "Saraah Mcglaklin",
        "Sarrah  Mclaclahan",
        "Saraah MMcclahlen",
        "Sarrah Mcglauklen",
        "Saraah Mcccloghlin",
        "Sarrah Mcgloucklann",
        "Sarraah Mclaukyn",
        "Sarahh Maclalahann",
        "SSarah Maglaphlin",
        "SSarah Mcclaughcllan",
        "Saraah Mcchlachann",
        "Saarah Mcgluacliin",
        "SSarah Maccllauchan",
        "Saraah Mclohlhann",
        "Saarah Maclaclaunn",
        "Saarah Mughlochlan",
        "Sarrah Mcgoclian",
        "Sarahh Mclafdja",
        "SSarah Mcglachelenn",
        "Sarrah Mcgglothan",
        "Saraah Mclaklpeen",
        "Sarrah Mcgloghlaiin",
        "Saraah Magluchalan",
        "Sarrah Meglloghlan",
        "SSarah Mcglocnan",
        "SSarah Mcluklinn",
        "Sarah Mcglocchilann",
        "Sarrah Mcglclainn",
        "SSarah Mcgloccklam",
        "Sarrah Mgclocklinn",
        "Sarahh Mcglaculainn",
        "SSarah MMclaquelin",
        "SSarah Maglauhlinn",
        "Saraah Mcglauhignn",
    ]
    print(f"{query}")
    for str_1 in str_1_list:
        print(f"{sim_string_fuzz(query, str_1, is_lower=False):.5f} - {str_1}")


if __name__ == "__main__":
    # test_sim_num()
    # test_dis_text()
    # test_closest()
    test_sim()
