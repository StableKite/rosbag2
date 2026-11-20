# Copyright 2022, Foxglove Technologies. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess
import sys
import sysconfig
import unittest

import common  # noqa

from rclpy.duration import Duration
from rclpy.time import Time
from rosbag2_py import (
    BagMetadata,
    ConverterOptions,
    FileInformation,
    StorageFilter,
    StorageOptions,
    TopicInformation,
    TopicMetadata,
)


class TestStorageStructs(unittest.TestCase):
    def test_metadata_views_survive_replacement(self):
        topic = TopicMetadata(0, 'original', 'std_msgs/msg/String', 'cdr')
        info = TopicInformation(topic, 1)
        bag = BagMetadata(topics_with_message_count=[info])
        view = bag.topics_with_message_count[0]
        assert view is bag.topics_with_message_count[0]
        view.topic_metadata.name = 'changed'
        assert bag.topics_with_message_count[0].topic_metadata.name == 'changed'

        topic_view = view.topic_metadata
        view.topic_metadata = topic
        assert topic_view is view.topic_metadata
        assert topic_view.name == 'original'
        bag.topics_with_message_count = []
        assert view.topic_metadata.name == 'original'
        assert bag.topics_with_message_count == []

    @unittest.skipUnless(sysconfig.get_config_var('Py_GIL_DISABLED'), 'needs free-threaded Python')
    def test_read_and_write_bag_without_gil(self):
        environment = dict(os.environ)
        environment.pop('PYTHON_GIL', None)
        subprocess.run([sys.executable, '-c', """
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

assert sys.flags.gil is None and not sys._is_gil_enabled()
import rosbag2_py as bag
from rclpy.serialization import deserialize_message, serialize_message
from std_msgs.msg import String

with TemporaryDirectory() as directory, ThreadPoolExecutor(max_workers=8) as pool:
    storage = bag.StorageOptions(uri=str(Path(directory) / 'bag'), storage_id='sqlite3')
    converter = bag.ConverterOptions('cdr', 'cdr')
    writer = bag.SequentialWriter()
    writer.open(storage, converter)
    writer.create_topic(bag.TopicMetadata(0, 'topic', 'std_msgs/msg/String', 'cdr'))

    def write(i):
        writer.write('topic', serialize_message(String(data=str(i))), i)

    list(pool.map(write, range(64)))
    writer.close()
    reader = bag.SequentialReader()
    reader.open(storage, converter)

    def read(_):
        topic, message, stamp, _ = reader.read_next_ext()
        assert topic == 'topic' and deserialize_message(message, String).data == str(stamp)
        return stamp

    assert sorted(pool.map(read, range(64))) == list(range(64))
    assert not reader.has_next()
    reader.close()
assert not sys._is_gil_enabled()
"""], env=environment, timeout=30, check=True)


    def test_bag_metadata_default_ctor(self):
        metadata = BagMetadata()
        assert metadata

    def test_converter_options_ctor(self):
        converter_options = ConverterOptions()
        assert converter_options

    def test_file_information_ctor(self):
        file_information = FileInformation(
            path='test_path',
            starting_time=Time(nanoseconds=1000),
            duration=Duration(),
            message_count=1234,
        )
        assert file_information

    def test_storage_options_ctor(self):
        storage_options = StorageOptions(uri='path')
        assert storage_options

    def test_storage_filter_ctor(self):
        storage_filter = StorageFilter()
        assert storage_filter

    def test_topic_metadata_ctor(self):
        topic_metadata = TopicMetadata(
            id=0,
            name='topic',
            type='msgs/Msg',
            serialization_format='format'
        )
        assert topic_metadata

    def test_topic_information_ctor(self):
        topic_information = TopicInformation(
            topic_metadata=TopicMetadata(
                id=0,
                name='topic',
                type='msgs/Msg',
                serialization_format='format'),
            message_count=10
        )
        assert topic_information

    def test_bag_metadata_ctor_named_args(self):
        duration = Duration(nanoseconds=200)
        starting_time = Time(nanoseconds=100)

        file_information = FileInformation(
            path='something',
            starting_time=starting_time,
            duration=duration,
            message_count=12)
        topic_information = TopicInformation(
            topic_metadata=TopicMetadata(
                id=0,
                name='topic',
                type='msgs/Msg',
                serialization_format='format'),
            message_count=10
        )

        metadata = BagMetadata(
            version=1,
            bag_size=2,
            storage_identifier='foo',
            relative_file_paths=['bar', 'baz'],
            files=[file_information],
            duration=duration,
            starting_time=starting_time,
            message_count=12,
            topics_with_message_count=[topic_information],
            compression_format='aaaa',
            compression_mode='bbbbb',
            custom_data={
                'keya': 'valuea',
                'keyb': 'valueb'
            }
        )
        assert metadata
