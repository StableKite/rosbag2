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

from common import get_rosbag_options

import pytest

from rclpy.serialization import deserialize_message, serialize_message
from rosbag2_py import (
    compression_mode_from_string,
    compression_mode_to_string,
    CompressionMode,
    CompressionOptions,
    SequentialCompressionReader,
    SequentialCompressionWriter,
    TopicMetadata,
)
from rosbag2_test_common import TESTED_STORAGE_IDS
from rosidl_runtime_py.utilities import get_message

from std_msgs.msg import String


def test_compression_mode_from_string():
    """Checks that we can cast a string to a compression mode."""
    assert CompressionMode.NONE == compression_mode_from_string('NONE')
    assert CompressionMode.MESSAGE == compression_mode_from_string('MESSAGE')
    assert CompressionMode.FILE == compression_mode_from_string('FILE')


def test_compression_mode_to_string():
    """Checks that we can cast a compression mode to a string."""
    assert 'NONE' == compression_mode_to_string(CompressionMode.NONE)
    assert 'MESSAGE' == compression_mode_to_string(CompressionMode.MESSAGE)
    assert 'FILE' == compression_mode_to_string(CompressionMode.FILE)


def test_compression_options():
    """Checks that we can construct a CompressionOptions class."""
    compression_options = CompressionOptions(
        compression_format='zstd',
        compression_mode=CompressionMode.MESSAGE,
        compression_queue_size=0,
        compression_threads=8)
    assert compression_options is not None
    assert compression_options.compression_format == 'zstd'
    assert compression_options.compression_mode == CompressionMode.MESSAGE
    assert compression_options.compression_queue_size == 0
    assert compression_options.compression_threads == 8


@pytest.mark.skipif(not sysconfig.get_config_var('Py_GIL_DISABLED'), reason='requires FT Python')
def test_free_threaded_compression_options():
    """Exercise one shared Python options object with the default GIL policy."""
    path = sys.modules[CompressionOptions.__module__].__file__
    probe = """
from concurrent.futures import ThreadPoolExecutor
from importlib.util import module_from_spec, spec_from_file_location
import sys
from threading import Barrier

assert sys.flags.gil is None and not sys._is_gil_enabled()
spec = spec_from_file_location('_compression_options', sys.argv[1])
module = module_from_spec(spec)
spec.loader.exec_module(module)
assert not sys._is_gil_enabled()

formats = {str(i) * 4096 for i in range(8)}
options = module.CompressionOptions(min(formats))
ready = Barrier(8, timeout=30)

def update(worker):
    ready.wait()
    value = str(worker) * 4096
    for _ in range(200):
        options.compression_format = value
        assert options.compression_format in formats
        options.compression_threads = worker
        assert 0 <= options.compression_threads < 8
        assert (module.compression_mode_from_string('message')
                == module.CompressionMode.MESSAGE)

with ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(update, range(8)))
assert not sys._is_gil_enabled()
"""
    subprocess.run([sys.executable, '-I', '-c', probe, path], check=True, timeout=90)


@pytest.mark.parametrize('storage_id', TESTED_STORAGE_IDS)
def test_sequential_compression(tmp_path, storage_id):
    """Checks that we can do a compressed write and read."""
    bag_path = os.path.join(tmp_path, 'tmp_sequential_compressed_write_test')

    storage_options, converter_options = get_rosbag_options(
        path=bag_path,
        storage_id=storage_id)

    compression_options = CompressionOptions(
        compression_format='zstd',
        compression_mode=CompressionMode.MESSAGE,
        compression_queue_size=0,
        compression_threads=1)

    writer = SequentialCompressionWriter(compression_options)
    writer.open(storage_options, converter_options)

    topic_name = '/chatter'
    topic_metadata = TopicMetadata(
        id=0,
        name=topic_name,
        type='std_msgs/msg/String',
        serialization_format='cdr')
    writer.create_topic(topic_metadata)

    for i in range(10):
        msg = String()
        msg.data = f'Hello, world! {str(i)}'
        time_stamp = i * 100

        writer.write(topic_name, serialize_message(msg), time_stamp)

    # close bag and create new storage instance
    del writer

    storage_options, converter_options = get_rosbag_options(bag_path, storage_id)

    reader = SequentialCompressionReader()
    reader.open(storage_options, converter_options)

    topic_types = reader.get_all_topics_and_types()

    # Create a map for quicker lookup
    type_map = {topic_types[i].name: topic_types[i].type for i in range(len(topic_types))}

    msg_counter = 0
    while reader.has_next():
        topic, data, recv_ts, send_ts = reader.read_next_ext()
        msg_type = get_message(type_map[topic])
        msg_deserialized = deserialize_message(data, msg_type)

        assert isinstance(msg_deserialized, String)
        assert msg_deserialized.data == f'Hello, world! {msg_counter}'
        assert recv_ts == msg_counter * 100

        # NOTE: Different behaviours when writing in mcap and sqlite3 for the send timestamp
        if 'sqlite3' == storage_id:
            assert send_ts == 0
        elif 'mcap' == storage_id:
            assert send_ts == msg_counter * 100

        msg_counter += 1
