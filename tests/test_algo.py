from time import time
from api.utilities import m_utils as ul


def get_time(func):
    start = time()
    func()
    print(f"{str(func)} - Time: {time() - start}")


def test():
    # get_time(test_list)
    get_time(test_gen_deletes)
    get_time(test_gen_deletes_2)


def test_gen_deletes():
    data = [
        "hope",
        "Phuc Nguyen",
        "Nguyen Tri Phuc",
        "Thuy Hang Nguyen Thi",
        "Amazon Prime",
    ] * 1000
    for d in data:
        tmp = ul.delete_edits_prefix(d, max_edit_dis=10, prefix_length=10)


def test_gen_deletes_2():
    data = [
        "k",
        "hope",
        "Phuc Nguyen",
        "Nguyen Tri Phuc",
        "Thuy Hang Nguyen Thi",
        "Amazon Prime",
        "a",
        "aa",
        "abasd",
        "aqlqn",
    ] * 1000
    for d in data:
        tmp = ul.delete_edits_prefix_2(d, max_edit_dis=10, prefix_length=1)
        # tmp2 = ul.delete_edits_prefix(d, max_edit_dis=3, prefix_length=1)
        # if tmp != tmp2:
        #     print(d)
