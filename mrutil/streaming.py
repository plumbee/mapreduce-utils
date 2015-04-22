# -----------------------------------------------------------------------------
# Copyright 2015 Plumbee Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# Modules
import itertools
import json
import operator
import os
import sys

KV_SEPARATOR = '\t'

INPUT_FORMAT_SEQUENCE_FILE = "SEQUENCE_FILE"
INPUT_FORMAT_TEXT_FILE = "TEXT_FILE"


# -----------------------------------------------------------------------------
# emit
# -----------------------------------------------------------------------------
def emit(key, values, delimiter='\t', stream=sys.stdout):
    """
    Emit a key value pair for processing by the MapReduce framework.
    :param key: The key to partition the data on. All keys of the same value
           will be processed by the same reducer.
    :param values: The data associated to the key as a iterable. All items of
           the iterable will be automatically converted to a suitable value
           for streaming.
    :param delimiter: The delimiter to be used when converting the values to a
           string.
    :param stream: The stream to write the output to, defaults to STDOUT.
    """
    def convert(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(',', ':'))  # Compact output
        elif isinstance(value, bool):
            return str(value).upper()
        else:
            return str(value)

    stream.write(''.join([
        str(key), KV_SEPARATOR,
        delimiter.join(map(convert, values)), os.linesep]))


# -----------------------------------------------------------------------------
# map_json_record_reader
# -----------------------------------------------------------------------------
def map_json_record_reader(stream=sys.stdin,
                           input_format=INPUT_FORMAT_SEQUENCE_FILE):
    """
    Generator function for reading a stream of JSON based records.
    :param stream: The stream to read data from, defaults to STDIN.
    :param input_format: The type of data being read.

    :return: A tuple containing the key and it's associated value. The value
             will be a decoded representation (an object) of the underlying
             streams JSON data.

             For TEXT_FILE based input, the key represents the offset of the
             data being processed.
    """
    offset = 0
    for line in stream:
        if input_format == INPUT_FORMAT_SEQUENCE_FILE:
            k, _, v = line.rstrip().partition(KV_SEPARATOR)
            yield (k, json.loads(v))
        elif input_format == INPUT_FORMAT_TEXT_FILE:
            yield (offset, json.loads(line.rstrip()))
            offset += len(line)
        else:
            raise RuntimeError("Unknown input_format '{}'".format(input_format))


# -----------------------------------------------------------------------------
# reduce_record_reader
# -----------------------------------------------------------------------------
def reduce_record_grouper(stream=sys.stdin,
                          key_func=operator.itemgetter(0),
                          delimiter='\t'):
    """
    Function that groups a stream of data together on a given key returning the
    key itself and the associated values as an iterator. NOTE: The input stream
    must be presorted!

    Example:
      Given the following stream of data:
        -> 100\tA,B,C,D
        -> 100\tE,F,G,H
        -> 200\t1,2,3,4
        -> 200\t5,6,7,8

      With the delimiter ',', yields:
        Key       Values
        -------------------------------
        <- 100    [[A,B,C,D],[E,F,G,H]]
        <- 200    [[1,2,3,4],[5,6,7,8]]

    :param stream: The stream to read data from, defaults to STDIN.
    :param key_func: The function used to compute the key on which to group
           data together.
    :param delimiter: The delimiter used to split the values associated to the
           key. This is typically the same as passed to emit() during the map
           phase.

    :return: A tuple containing the value of the grouping operation and all the
             values associated to that group as a list of lists.
    """
    def reader():
        while True:
            k, _, v = stream.next().rstrip().partition(KV_SEPARATOR)
            yield k, v

    for key, values in itertools.groupby(reader(), key=key_func):
        if delimiter:
            yield (key, list(value.split(delimiter) for key, value in values))
        else:
            yield (key, list(value for key, value in values))


# -----------------------------------------------------------------------------
# time_based_sessionizer
# -----------------------------------------------------------------------------
def time_based_sessionizer(events, inactivity_timeout,
                           key_func=operator.itemgetter(0)):
    """
    Sessionize/SubGroup a collection of events based on a configurable period
    of inactivity. Note: The event input data must be presorted and must
    contain a field which represents a timestamp.

    :param events: The collection of events to be processed.
    :param inactivity_timeout: The timeout between events before a new session
           is created.
    :param key_func: The function used to return the timestamp within the
           collection of events.

    :return: A tuple containing the session start and end timestamps along
             with the events that contributed to that session in a list.
    """
    data = []
    prev_timestamp = None
    for row in events:
        curr_timestamp = int(key_func(row))
        if prev_timestamp and curr_timestamp - prev_timestamp > inactivity_timeout:
            yield (int(key_func(data[0])), int(key_func(data[-1])), data)
            data = []
        data.append(row)
        prev_timestamp = curr_timestamp

    if len(data):
        yield (int(key_func(data[0])), int(key_func(data[-1])), data)

