// Copyright 2020 Open Source Robotics Foundation, Inc.
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

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "rosbag2_compression/sequential_compression_reader.hpp"
#include "rosbag2_cpp/converter_options.hpp"
#include "rosbag2_cpp/plugins/plugin_utils.hpp"
#include "rosbag2_cpp/readers/sequential_reader.hpp"
#include "rosbag2_cpp/reader.hpp"
#include "rosbag2_storage/storage_interfaces/read_only_interface.hpp"
#include "rosbag2_storage/storage_interfaces/read_write_interface.hpp"
#include "rosbag2_storage/storage_filter.hpp"
#include "rosbag2_storage/storage_options.hpp"
#include "rosbag2_storage/topic_metadata.hpp"

#include "./pybind11.hpp"
#include "value_properties.hpp"
#include "storage_values.hpp"

namespace rosbag2_py
{

template<typename T>
class Reader : public rosbag2_cpp::Reader
{
public:
  Reader()
  : rosbag2_cpp::Reader(std::make_unique<T>())
  {
  }

  template<typename Result, typename ... Args>
  static auto synchronized(Result (rosbag2_cpp::Reader::*method)(Args ...))
  {
    return [method](Reader & reader, Args ... args) {
             std::lock_guard<std::mutex> lock(reader.mutex_);
             return (reader.*method)(args ...);
           };
  }

  void open(
    const rosbag2_storage::StorageOptions & storage_options,
    const rosbag2_cpp::ConverterOptions & converter_options)
  {
    const auto storage = value_snapshot(storage_options);
    const auto converter = value_snapshot(converter_options);
    std::lock_guard<std::mutex> lock(mutex_);
    rosbag2_cpp::Reader::open(storage, converter);
  }

  bool set_read_order(const rosbag2_storage::ReadOrder & order)
  {
    const auto snapshot = value_snapshot(order);
    std::lock_guard<std::mutex> lock(mutex_);
    return rosbag2_cpp::Reader::set_read_order(snapshot);
  }

  void set_filter(const rosbag2_storage::StorageFilter & filter)
  {
    const auto snapshot = value_snapshot(filter);
    std::lock_guard<std::mutex> lock(mutex_);
    rosbag2_cpp::Reader::set_filter(snapshot);
  }

  PyBagMetadata get_metadata() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return PyBagMetadata(rosbag2_cpp::Reader::get_metadata());
  }

  SharedValues<PyTopicMetadata> get_all_topics_and_types() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return share_values<PyTopicMetadata>(rosbag2_cpp::Reader::get_all_topics_and_types());
  }

  std::vector<rosbag2_storage::MessageDefinition> get_all_message_definitions()
  {
    std::vector<rosbag2_storage::MessageDefinition> definitions;
    std::lock_guard<std::mutex> lock(mutex_);
    rosbag2_cpp::Reader::get_all_message_definitions(definitions);
    return definitions;
  }

  /// Return a tuple containing the topic name, serialized message and receive timestamp.
  pybind11::tuple read_next()
  {
    if (PyErr_WarnEx(PyExc_DeprecationWarning,
      "Use read_next_ext() which returns both send and receive timestamps instead "
      "of just receive timestamp.", 1) != 0)
    {
      throw pybind11::error_already_set();
    }

    const auto message = take_next();
    return pybind11::make_tuple(
      message.topic, pybind11::bytes(message.data), message.receive_timestamp);
  }

  /// Return both the receive and send timestamps with the serialized message.
  pybind11::tuple read_next_ext()
  {
    const auto message = take_next();
    return pybind11::make_tuple(
      message.topic, pybind11::bytes(message.data),
      message.receive_timestamp, message.send_timestamp);
  }

private:
  struct Message
  {
    std::string topic;
    std::string data;
    rcutils_time_point_value_t receive_timestamp;
    rcutils_time_point_value_t send_timestamp;
  };

  Message take_next()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto message = rosbag2_cpp::Reader::read_next();
    const auto & data = *message->serialized_data;
    std::string buffer;
    if (data.buffer_length != 0) {
      buffer.assign(reinterpret_cast<const char *>(data.buffer), data.buffer_length);
    }
    return {message->topic_name, std::move(buffer), message->recv_timestamp, message->send_timestamp};
  }

  mutable std::mutex mutex_;
};

std::unordered_set<std::string> get_registered_readers()
{
  std::unordered_set<std::string> combined_plugins = rosbag2_cpp::plugins::get_class_plugins
    <rosbag2_storage::storage_interfaces::ReadWriteInterface>();

  std::unordered_set<std::string> read_only_plugins = rosbag2_cpp::plugins::get_class_plugins
    <rosbag2_storage::storage_interfaces::ReadOnlyInterface>();

  // Merge read/write and read-only plugin sets
  combined_plugins.insert(read_only_plugins.begin(), read_only_plugins.end());

  return combined_plugins;
}

}  // namespace rosbag2_py

using PyReader = rosbag2_py::Reader<rosbag2_cpp::readers::SequentialReader>;
using PyCompressionReader = rosbag2_py::Reader<rosbag2_compression::SequentialCompressionReader>;

template<typename PyReaderType>
void bind_reader(pybind11::module_ & module, const char * name)
{
  using NativeReader = rosbag2_cpp::Reader;
  pybind11::class_<PyReaderType>(module, name)
  .def(pybind11::init())
  .def(
    "open_uri",
    PyReaderType::synchronized(
      pybind11::overload_cast<const std::string &>(&NativeReader::open)))
  .def("open", &PyReaderType::open)
  .def("close", PyReaderType::synchronized(&NativeReader::close))
  .def("set_read_order", &PyReaderType::set_read_order)
  .def("read_next", &PyReaderType::read_next)
  .def("read_next_ext", &PyReaderType::read_next_ext)
  .def("has_next", PyReaderType::synchronized(&NativeReader::has_next))
  .def("get_metadata", &PyReaderType::get_metadata)
  .def("get_all_topics_and_types", &PyReaderType::get_all_topics_and_types)
  .def("get_all_message_definitions", &PyReaderType::get_all_message_definitions)
  .def("set_filter", &PyReaderType::set_filter)
  .def("reset_filter", PyReaderType::synchronized(&NativeReader::reset_filter))
  .def("seek", PyReaderType::synchronized(&NativeReader::seek));
}

PYBIND11_MODULE(_reader, m, pybind11::mod_gil_not_used()) {
  m.doc() = "Python wrapper of the rosbag2_cpp reader API";

  bind_reader<PyReader>(m, "SequentialReader");
  bind_reader<PyCompressionReader>(m, "SequentialCompressionReader");
  m.def(
    "get_registered_readers",
    &rosbag2_py::get_registered_readers,
    "Returns list of discovered plugins that support rosbag2 playback.");
}
