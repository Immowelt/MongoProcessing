# Mongo Processing

A simple library for developing workflows using MongoDB Change Streams.

## Usage
##### Set up a MongoDB repository
```python
uri = 'mongodb://user:pw@ip:port,ip:port,ip:port/admin?replicaSet=rs1'
repo = MongoRepository(uri, 'db', 'collection')
```

##### Listen to inserted documents
The following code sets up a watch that triggers whenever a new document is inserted in the collection.
The process writes a new property 'a' to the document.
```python
def acknowledge_one(doc):
    return True


def process_one(doc):
    results = {'a': randint(0, 9)}
    return True, results


created_watch = WatchBuilder(repo).listen_to('insert')
created_watch.start_worker('one', acknowledge_one, process_one)
```

##### Listen to previous processes
This code demonstrates how to execute a process after a previous process.
```python
def acknowledge_two(doc):
    return True


def process_two(doc):
    a = doc['one']['a']
    results = {'b': 2 * a}

    if a < 4:
        results['c'] = randint(0, 9)

    return True, results


one_requirement = ProcessRequirement('one')
one_watch = WatchBuilder(repo).listen_to('update').add_process_requirement(one_requirement)
one_watch.start_worker('two', acknowledge_two, process_two)
```

##### Additional conditions
In this example, the event will only be received if process 'two' has a result with key 'c'.
Additionally, process 'three' should only execute if 'c' is greater than 2.

```python
def acknowledge_three(doc):
    return doc['two']['c'] > 2


def process_three(doc):
    a = doc['one']['a']
    b = doc['two']['b']
    c = doc['two']['c']
    
    results = {'sum': a + b + c}

    return True, results


two_requirement = ProcessRequirement('two', True, 'c')
two_watch = WatchBuilder(repo).listen_to('update').add_process_requirement(two_requirement)
two_watch.start_worker('three', acknowledge_three, process_three)
```

MongoProcessing automatically records the start and end time of processes.
In the example above, the `trigger_if_rerun` argument of the `ProcessRequirement` is set to true.
That means that if process 'two' runs again, process 'three' will also re-run.
