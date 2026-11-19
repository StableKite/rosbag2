# Copyright 2021 DCS Corporation, All Rights Reserved.
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
#
# DISTRIBUTION A. Approved for public release; distribution unlimited.
# OPSEC #4584.
#
# Delivered to the U.S. Government with Unlimited Rights, as defined in DFARS
# Part 252.227-7013 or 7014 (Feb 2014).
#
# This notice must appear in all copies of this file and its derivatives.

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from shutil import copytree
import subprocess
import sys
import sysconfig

from common import get_rosbag_options

import pytest

import rosbag2_py
from rosbag2_test_common import TESTED_STORAGE_IDS


RESOURCES_PATH = Path(os.environ['ROSBAG2_PY_TEST_RESOURCES_DIR'])


@pytest.mark.parametrize('storage_id', TESTED_STORAGE_IDS)
def test_reindexer_multiple_files(storage_id):
    bag_path = RESOURCES_PATH / storage_id / 'reindex_test_bags' / 'multiple_files'
    result_path = bag_path / 'metadata.yaml'

    storage_options, _ = get_rosbag_options(str(bag_path), storage_id=storage_id)
    reindexer = rosbag2_py.Reindexer()
    reindexer.reindex(storage_options)

    assert result_path.exists()

    try:
        result_path.unlink()
    except FileNotFoundError:
        pass


@pytest.mark.skipif(not sysconfig.get_config_var('Py_GIL_DISABLED'), reason='requires FT Python')
def test_reindexer_module_keeps_free_threading():
    path = sys.modules[rosbag2_py.Reindexer.__module__].__file__
    probe = """
from importlib.util import module_from_spec, spec_from_file_location
import sys

assert sys.flags.gil is None and not sys._is_gil_enabled()
spec = spec_from_file_location('_reindexer', sys.argv[1])
module = module_from_spec(spec)
spec.loader.exec_module(module)
assert not sys._is_gil_enabled()
assert module.Reindexer() is not None
"""
    subprocess.run([sys.executable, '-I', '-c', probe, path], check=True, timeout=30)


@pytest.mark.parametrize('storage_id', TESTED_STORAGE_IDS)
def test_reindexer_can_retry_and_be_shared(storage_id, tmp_path):
    reindexer = rosbag2_py.Reindexer()
    missing = tmp_path / 'empty'
    missing.mkdir()
    options, _ = get_rosbag_options(str(missing), storage_id=storage_id)
    with pytest.raises(RuntimeError, match='Empty directory'):
        reindexer.reindex(options)

    original = RESOURCES_PATH / storage_id / 'reindex_test_bags' / 'multiple_files'
    directories = [copytree(original, tmp_path / str(i)) for i in range(8)]

    def reindex(directory):
        options, _ = get_rosbag_options(str(directory), storage_id=storage_id)
        reindexer.reindex(options)
        metadata = rosbag2_py.Info().read_metadata(str(directory), storage_id)
        assert len(metadata.relative_file_paths) == 3
        assert metadata.message_count > 0

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(reindex, directories))
