# -*- coding: utf-8 -*-
import logging

class OperationTypeDependency(object):
    def __init__(self, operation_types):
        self.operation_types = operation_types

    def _add_match_condition(self, name, match, outer_and_list, or_filters, op_type):
        pass

class RequiredKeyDependency(object):
    def __init__(self, key, operation_types):
        self.key = key
        self.operation_types = operation_types

    def _add_match_condition(self, name, match, outer_and_list, or_filters, op_type):
        match['fullDocument.{}'.format(self.key)] = {'$exists': True}


class KeyValueDependency(object):
    def __init__(self, key, value, operation_types):
        self.key = key
        self.value = value
        self.operation_types = operation_types

    def _add_match_condition(self, name, match, outer_and_list, or_filters, op_type):
        match['fullDocument.{}'.format(self.key)] = self.value

class ProcessDependency(object):
    def __init__(self, process_name, operation_types = ['update'], trigger_if_rerun=True, *required_results):
        """
        Create a new process requirement.
        :param process_name: The process that must have finished successfully
        :param trigger_if_rerun: If true, re-run the new process if this old process has been run again
        :param required_results: The property names of all results that the process must have written
        """
        self.process_name = process_name
        self.trigger_if_rerun = trigger_if_rerun
        self.required_results = required_results
        self.operation_types = operation_types

    def _equals_dot_workaround(self, field, value):
        or_dict = {
            "$or": [
                {
                    '$expr': {
                        '$eq': [
                            {
                                '$let': {
                                    'vars': {
                                        'foo': {
                                            '$arrayElemAt': [
                                                {
                                                    '$filter': {
                                                        'input': {
                                                            '$objectToArray': '$updateDescription.updatedFields'
                                                        },
                                                        'cond': {'$eq': [field, '$$this.k']}
                                                    }
                                                },
                                                0
                                            ]
                                        }
                                    },
                                    'in': '$$foo.v'
                                }
                            },
                            value
                        ]
                    }
                },
                {'updateDescription.updatedFields.{}'.format(field): value}
            ]
        }

        return or_dict

    def _add_match_condition(self, name, match, outer_and_list, or_filters, op_type):
        match['fullDocument.{}.success'.format(self.process_name)] = True

        if self.operation_type == 'update':
            success_true = self._equals_dot_workaround('{}.success'.format(self.process_name), True)
            outer_and_list.append(success_true)

        for result in self.required_results:
            match['fullDocument.{}.{}'.format(self.process_name, result)] = {'$exists': True}

        if self.trigger_if_rerun:
            or_filters.append({
                '$expr': {
                    '$and': [{
                        '$gt': [
                            '$fullDocument.{}.endTime'.format(self.process_name),
                            '$fullDocument.{}.endTime'.format(name)
                        ]}, {
                        '$gt': [
                            '$fullDocument.{}.startTime'.format(self.process_name),
                            '$fullDocument.{}.startTime'.format(name)
                        ]},
                    ]}
            })


class MultipleDependency(object):
    def __init__(self, dependencies):
        self._children = dependencies

    @property
    def operation_types(self):
        return set([y for x in self._children for y in x.operation_types])

    def add_dependency(self, dependency):
        self._children.extend(dependency)
        if 'update' in self.operation_types and 'replace' in self.operation_types:
            logging.warning('MongoWatch contains both update and replace dependencies which can lead to data loss')

    def _add_match_condition(self, name, match, outer_and_list, or_filters, op_type):
        for dependency in self._children:
            if op_type in dependency.operation_types:
                dependency._add_match_condition(name, match, outer_and_list, or_filters)

    def __len__(self):
        return len(self._children)