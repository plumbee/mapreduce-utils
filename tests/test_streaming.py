#!/usr/bin/env python

# -----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# -----------------------------------------------------------------------------

# Modules
import unittest
import StringIO

from mrutil import streaming


# -----------------------------------------------------------------------------
# TestEmit
# -----------------------------------------------------------------------------
class TestEmit(unittest.TestCase):

    def setUp(self):
        self.output_stream=StringIO.StringIO()

    def tearDown(self):
        self.output_stream.close()

    def testDict(self):
        streaming.emit('dict', [{"key": "value"}], stream=self.output_stream)
        self.assertEqual(self.output_stream.getvalue(),
            'dict\t{"key":"value"}\n')

    def testBoolean(self):
        streaming.emit('boolean', [True, False], stream=self.output_stream)
        self.assertEqual(self.output_stream.getvalue(),
            'boolean\tTRUE\tFALSE\n')


# -----------------------------------------------------------------------------
# TestMapJsonRecordReader
# -----------------------------------------------------------------------------
class TestMapJsonRecordReader(unittest.TestCase):

    def setUp(self):
        self.input_stream = StringIO.StringIO()

    def tearDown(self):
        self.input_stream.close()

    def testSequenceFile(self):
        self.input_stream.write('1\t{"A": "B"}\n')
        self.input_stream.write('2\t{"C": "D"}\n')
        self.input_stream.seek(0)

        output = streaming.map_json_record_reader(
            stream=self.input_stream,
            input_format=streaming.INPUT_FORMAT_SEQUENCE_FILE)
        self.assertEqual(output.next(), ("1", {"A": "B"}))
        self.assertEqual(output.next(), ("2", {"C": "D"}))
        self.assertRaises(StopIteration, next, output)

    def testTextFile(self):
        self.input_stream.write('{"E": "F"}\n')
        self.input_stream.write('{"G": "H"}\n')
        self.input_stream.seek(0)

        output = streaming.map_json_record_reader(
            stream=self.input_stream,
            input_format=streaming.INPUT_FORMAT_TEXT_FILE)
        self.assertEqual(output.next(), (0,  {"E": "F"}))
        self.assertEqual(output.next(), (11, {"G": "H"}))
        self.assertRaises(StopIteration, next, output)


# -----------------------------------------------------------------------------
# TestReduceRecordGrouper
# -----------------------------------------------------------------------------
class TestReduceRecordGrouper(unittest.TestCase):

    def setUp(self):
        self.input_stream = StringIO.StringIO()

    def tearDown(self):
        self.input_stream.close()

    def testDelimiter(self):
        self.input_stream.write("100\tA,B,C,D\n")
        self.input_stream.write("100\tE,F,G,H\n")
        self.input_stream.write("200\t1,2,3,4\n")
        self.input_stream.write("200\t5,6,7,8\n")
        self.input_stream.seek(0)

        output = streaming.reduce_record_grouper(
            stream=self.input_stream, delimiter=",")
        self.assertEqual(output.next(), (
            "100", [["A", "B", "C", "D"], ["E", "F", "G", "H"]]))
        self.assertEqual(output.next(), (
            "200", [["1", "2", "3", "4"], ["5", "6", "7", "8"]]))
        self.assertRaises(StopIteration, next, output)


# -----------------------------------------------------------------------------
# TestTimeBasedSessionizer
# -----------------------------------------------------------------------------
class TestTimeBasedSessionizer(unittest.TestCase):

    def testSessionizer(self):
        output = streaming.time_based_sessionizer(
            [[1, "A"], [2, "B"], [10, "C"], [16, "D"]], 5)
        self.assertEqual(output.next(), (1,  2,  [[1,  "A"], [2, "B"]]))
        self.assertEqual(output.next(), (10, 10, [[10, "C"]]))
        self.assertEqual(output.next(), (16, 16, [[16, "D"]]))
        self.assertRaises(StopIteration, next, output)


# -----------------------------------------------------------------------------
# Bootstrap
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main()