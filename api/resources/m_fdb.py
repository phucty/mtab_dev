import itertools
import os
import traceback

import fdb
import m_config as cf
import fdb.tuple

fdb.api_version(630)


####################################
##        Initialization          ##
####################################

# Data model:
# ('attends', student, class) = ''
# ('class', class_name) = seats_left

db = fdb.open()
db.options.set_transaction_timeout(60000)  # 60,000 ms = 1 minute
db.options.set_transaction_retry_limit(100)
scheduling = fdb.directory.create_or_open(db, ("scheduling",))
course = scheduling["class"]
attends = scheduling["attends"]


@fdb.transactional
def add_class(tr, c):
    tr[course.pack((c,))] = fdb.tuple.pack((100,))


# Generate 1,620 classes like '9:00 chem for dummies'
levels = [
    "intro",
    "for dummies",
    "remedial",
    "101",
    "201",
    "301",
    "mastery",
    "lab",
    "seminar",
]
types = [
    "chem",
    "bio",
    "cs",
    "geometry",
    "calc",
    "alg",
    "film",
    "music",
    "art",
    "dance",
]
times = [str(h) + ":00" for h in range(0, 100)]
class_combos = itertools.product(times, types, levels)
class_names = [" ".join(tup) for tup in class_combos]


@fdb.transactional
def init(tr):
    del tr[scheduling.range(())]  # Clear the directory
    for class_name in class_names:
        add_class(tr, class_name)


####################################
##  Class Scheduling Functions    ##
####################################


@fdb.transactional
def available_classes(tr):
    return [
        course.unpack(k)[0] for k, v in tr[course.range(())] if fdb.tuple.unpack(v)[0]
    ]


@fdb.transactional
def signup(tr, s, c):
    rec = attends.pack((s, c))
    if tr[rec].present():
        return  # already signed up

    seats_left = fdb.tuple.unpack(tr[course.pack((c,))])[0]
    if not seats_left:
        raise Exception("No remaining seats")

    classes = tr[attends.range((s,))]
    if len(list(classes)) == 5:
        raise Exception("Too many classes")

    tr[course.pack((c,))] = fdb.tuple.pack((seats_left - 1,))
    tr[rec] = b""


@fdb.transactional
def drop(tr, s, c):
    rec = attends.pack((s, c))
    if not tr[rec].present():
        return  # not taking this class
    tr[course.pack((c,))] = fdb.tuple.pack(
        (fdb.tuple.unpack(tr[course.pack((c,))])[0] + 1,)
    )
    del tr[rec]


@fdb.transactional
def switch(tr, s, old_c, new_c):
    drop(tr, s, old_c)
    signup(tr, s, new_c)


####################################
##           Testing              ##
####################################

import random
import threading


def indecisive_student(i, ops):
    student_ID = "s{:d}".format(i)
    all_classes = class_names
    my_classes = []

    for i in range(ops):
        class_count = len(my_classes)
        moods = []
        if class_count:
            moods.extend(["drop", "switch"])
        if class_count < 5:
            moods.append("add")
        mood = random.choice(moods)

        try:
            if not all_classes:
                all_classes = available_classes(db)
            if mood == "add":
                c = random.choice(all_classes)
                signup(db, student_ID, c)
                my_classes.append(c)
            elif mood == "drop":
                c = random.choice(my_classes)
                drop(db, student_ID, c)
                my_classes.remove(c)
            elif mood == "switch":
                old_c = random.choice(my_classes)
                new_c = random.choice(all_classes)
                switch(db, student_ID, old_c, new_c)
                my_classes.remove(old_c)
                my_classes.append(new_c)
        except Exception as e:
            traceback.print_exc()
            print("Need to recheck available classes.")
            all_classes = []


def run(students, ops_per_student):
    threads = [
        threading.Thread(target=indecisive_student, args=(i, ops_per_student))
        for i in range(students)
    ]
    for thr in threads:
        thr.start()
    for thr in threads:
        thr.join()
    print("Ran {} transactions".format(students * ops_per_student))


if __name__ == "__main__":
    init(db)
    print("initialized")
    run(1000000, 10)
