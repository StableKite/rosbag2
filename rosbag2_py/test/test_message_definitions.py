# Copyright 2024 Intrinsic Innovation LLC. All rights reserved.
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

from concurrent.futures import ThreadPoolExecutor
import subprocess
import sys
import sysconfig
from threading import Barrier
import warnings

import pytest
from rosbag2_py import LocalMessageDefinitionSource


def test_local_message_definition_source_no_crash_on_bad_name():
    source = LocalMessageDefinitionSource()
    result = source.get_full_text_ext('some_pkg', 'some_topic')
    assert result.encoding == 'unknown'
    assert result.encoded_message_definition == ''
    assert result.topic_type == 'some_pkg'


def test_local_message_definition_source_can_find_msg_deps():
    source = LocalMessageDefinitionSource()
    result = source.get_full_text_ext('rosbag2_test_msgdefs/ComplexMsg', '/complex_msg')
    assert result.encoding == 'ros2msg'
    assert result.encoded_message_definition == (
        'rosbag2_test_msgdefs/BasicMsg b\n'
        '\n'
        '================================================================================\n'
        'MSG: rosbag2_test_msgdefs/BasicMsg\n'
        'float32 c\n'
    )


def test_local_message_definition_source_can_find_srv_deps_in_msg():
    source = LocalMessageDefinitionSource()
    result = source.get_full_text_ext('rosbag2_test_msgdefs/srv/ComplexSrvMsg', '/complex_msg')
    assert result.encoding == 'ros2msg'
    assert result.topic_type == 'rosbag2_test_msgdefs/srv/ComplexSrvMsg'
    assert result.encoded_message_definition == (
        '================================================================================\n'
        'SRV: rosbag2_test_msgdefs/srv/ComplexSrvMsg\n'
        'rosbag2_test_msgdefs/BasicMsg req\n'
        '---\n'
        'rosbag2_test_msgdefs/BasicMsg resp\n'
        '\n'
        '================================================================================\n'
        'MSG: rosbag2_test_msgdefs/BasicMsg\n'
        'float32 c\n'
    )


def test_local_message_definition_source_can_find_srv_deps_in_idl():
    source = LocalMessageDefinitionSource()
    result = source.get_full_text_ext('rosbag2_test_msgdefs/srv/ComplexSrvIdl', '/complex_msg')
    assert result.encoding == 'ros2idl'
    assert result.topic_type == 'rosbag2_test_msgdefs/srv/ComplexSrvIdl'
    assert result.encoded_message_definition == (
        '================================================================================\n'
        'SRV: rosbag2_test_msgdefs/srv/ComplexSrvIdl\n'
        'rosbag2_test_msgdefs/BasicIdl req\n'
        '---\n'
        'rosbag2_test_msgdefs/BasicIdl resp\n'
        '\n'
        '================================================================================\n'
        'MSG: rosbag2_test_msgdefs/BasicIdl\n'
        '\n'
        '================================================================================\n'
        'IDL: rosbag2_test_msgdefs/BasicIdl\n'
        'module rosbag2_test_msgdefs {\n'
        '  module msg {\n'
        '    struct BasicIdl {\n'
        '        float x;\n'
        '    };\n'
        '  };\n'
        '};\n'
    )


def test_local_message_definition_source_can_find_idl_deps():
    source = LocalMessageDefinitionSource()
    result = source.get_full_text_ext('rosbag2_test_msgdefs/msg/ComplexIdl', '/complex_idl')
    assert result.encoding == 'ros2idl'
    assert result.topic_type == 'rosbag2_test_msgdefs/msg/ComplexIdl'
    assert result.encoded_message_definition == (
        '================================================================================\n'
        'IDL: rosbag2_test_msgdefs/msg/ComplexIdl\n'
        '#include "rosbag2_test_msgdefs/msg/BasicIdl.idl"\n'
        '\n'
        'module rosbag2_test_msgdefs {\n'
        '  module msg {\n'
        '    struct ComplexIdl {\n'
        '      rosbag2_test_msgdefs::msg::BasicIdl a;\n'
        '    };\n'
        '  };\n'
        '};\n'
        '\n'
        '================================================================================\n'
        'IDL: rosbag2_test_msgdefs/msg/BasicIdl\n'
        'module rosbag2_test_msgdefs {\n'
        '  module msg {\n'
        '    struct BasicIdl {\n'
        '        float x;\n'
        '    };\n'
        '  };\n'
        '};\n'
    )


def test_local_message_definition_source_can_resolve_msg_with_idl_deps():
    source = LocalMessageDefinitionSource()
    result = source.get_full_text_ext(
        'rosbag2_test_msgdefs/msg/ComplexMsgDependsOnIdl', '/complex_msg_depends_on_idl')
    assert result.encoding == 'ros2idl'
    assert result.topic_type == 'rosbag2_test_msgdefs/msg/ComplexMsgDependsOnIdl'
    assert result.encoded_message_definition == (
        '================================================================================\n'
        'IDL: rosbag2_test_msgdefs/msg/ComplexMsgDependsOnIdl\n'
        '// generated from rosidl_adapter/resource/msg.idl.em\n'
        '// with input from rosbag2_test_msgdefs/msg/ComplexMsgDependsOnIdl.msg\n'
        '// generated code does not contain a copyright notice\n'
        '\n'
        '#include "rosbag2_test_msgdefs/msg/BasicIdl.idl"\n'
        '\n'
        'module rosbag2_test_msgdefs {\n'
        '  module msg {\n'
        '    struct ComplexMsgDependsOnIdl {\n'
        '      rosbag2_test_msgdefs::msg::BasicIdl a;\n'
        '    };\n'
        '  };\n'
        '};\n'
        '\n'
        '================================================================================\n'
        'IDL: rosbag2_test_msgdefs/msg/BasicIdl\n'
        'module rosbag2_test_msgdefs {\n'
        '  module msg {\n'
        '    struct BasicIdl {\n'
        '        float x;\n'
        '    };\n'
        '  };\n'
        '};\n'
    )


@pytest.mark.skipif(not sysconfig.get_config_var('Py_GIL_DISABLED'), reason='requires FT Python')
def test_message_definition_module_keeps_free_threading():
    path = sys.modules[LocalMessageDefinitionSource.__module__].__file__
    probe = """
from importlib.util import module_from_spec, spec_from_file_location
import sys

assert sys.flags.gil is None and not sys._is_gil_enabled()
spec = spec_from_file_location('_message_definitions', sys.argv[1])
module = module_from_spec(spec)
spec.loader.exec_module(module)
assert not sys._is_gil_enabled()
assert module.LocalMessageDefinitionSource() is not None
"""
    subprocess.run([sys.executable, '-I', '-c', probe, path], check=True, timeout=30)


def test_shared_message_definition_source():
    message_types = ('rosbag2_test_msgdefs/ComplexMsg', 'rosbag2_test_msgdefs/msg/ComplexIdl')
    source = LocalMessageDefinitionSource()
    ready = Barrier(8, timeout=30)

    def read(worker):
        message = message_types[worker % len(message_types)]
        ready.wait()
        for _ in range(24):
            result = source.get_full_text_ext(message, '/parallel')
            assert result.encoding in ('ros2msg', 'ros2idl')
            assert 'rosbag2_test_msgdefs' in result.encoded_message_definition

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(read, range(8)))


def test_message_definition_deprecation_error_is_preserved():
    source = LocalMessageDefinitionSource()
    with warnings.catch_warnings():
        warnings.simplefilter('error', DeprecationWarning)
        with pytest.raises(DeprecationWarning, match='get_full_text_ext'):
            source.get_full_text('rosbag2_test_msgdefs/ComplexMsg')
