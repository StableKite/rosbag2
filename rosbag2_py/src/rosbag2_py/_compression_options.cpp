// Copyright 2023 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <string>
#include <utility>

#include "rosbag2_compression/compression_options.hpp"

#include "./compression_options.hpp"
#include "./pybind11.hpp"

using CompressionMode = rosbag2_compression::CompressionMode;
using rosbag2_py::CompressionOptions;
using NativeOptions = CompressionOptions::Native;

PYBIND11_MODULE(_compression_options, m, pybind11::mod_gil_not_used()) {
  m.doc() = "Python wrapper of the rosbag2_compression API";

  pybind11::enum_<CompressionMode>(m, "CompressionMode")
  .value("NONE", CompressionMode::NONE)
  .value("FILE", CompressionMode::FILE)
  .value("MESSAGE", CompressionMode::MESSAGE)
  .export_values();

  pybind11::class_<CompressionOptions>(m, "CompressionOptions")
  .def(
    pybind11::init<std::string, CompressionMode, uint64_t, uint64_t>(),
    pybind11::arg("compression_format") = NativeOptions{}.compression_format,
    pybind11::arg("compression_mode") = NativeOptions{}.compression_mode,
    pybind11::arg("compression_queue_size") = NativeOptions{}.compression_queue_size,
    pybind11::arg("compression_threads") = NativeOptions{}.compression_threads)
  .def_property(
    "compression_format",
    [](const CompressionOptions & options) {return options.get(&NativeOptions::compression_format);},
    [](CompressionOptions & options, std::string value) {
      options.set(&NativeOptions::compression_format, std::move(value));
    })
  .def_property(
    "compression_mode",
    [](const CompressionOptions & options) {return options.get(&NativeOptions::compression_mode);},
    [](CompressionOptions & options, CompressionMode value) {
      options.set(&NativeOptions::compression_mode, value);
    })
  .def_property(
    "compression_queue_size",
    [](const CompressionOptions & options) {
      return options.get(&NativeOptions::compression_queue_size);
    },
    [](CompressionOptions & options, uint64_t value) {
      options.set(&NativeOptions::compression_queue_size, value);
    })
  .def_property(
    "compression_threads",
    [](const CompressionOptions & options) {
      return options.get(&NativeOptions::compression_threads);
    },
    [](CompressionOptions & options, uint64_t value) {
      options.set(&NativeOptions::compression_threads, value);
    });

  m.def(
    "compression_mode_from_string",
    &rosbag2_compression::compression_mode_from_string,
    "Converts a string into a rosbag2_compression::CompressionMode enum.");

  m.def(
    "compression_mode_to_string",
    &rosbag2_compression::compression_mode_to_string,
    "Converts a rosbag2_compression::CompressionMode enum into a string");
}
