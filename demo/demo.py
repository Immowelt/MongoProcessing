from random import randint

from mongoprocessing import MongoRepository, MongoWatch, ProcessRequirement

uri = 'mongodb://user:pw@ip:port,ip:port,ip:port/admin?replicaSet=rs1'
repo = MongoRepository(uri, 'demo', 'items')


def acknowledge_one(doc):
    return True


def process_one(doc):
    results = {'a': randint(0, 9)}
    return True, results


created_watch = MongoWatch(repo, 'insert')
created_watch.start_worker('one', acknowledge_one, process_one)


def acknowledge_two(doc):
    return True


def process_two(doc):
    a = doc['one']['a']
    results = {'b': 2 * a}

    if a < 4:
        results['c'] = randint(0, 9)

    return True, results


one_requirement = ProcessRequirement('one')
one_watch = MongoWatch(repo, 'update', one_requirement)
one_watch.start_worker('two', acknowledge_two, process_two)


def acknowledge_three(doc):
    return doc['two']['c'] > 2


def process_three(doc):
    a = doc['one']['a']
    b = doc['two']['b']
    c = doc['two']['c']

    results = {'sum': a + b + c}

    return True, results


two_requirement = ProcessRequirement('two', True, 'c')
two_watch = MongoWatch(repo, 'update', two_requirement)
two_watch.start_worker('three', acknowledge_three, process_three)
