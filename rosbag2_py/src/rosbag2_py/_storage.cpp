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

#include <string>
#include <utility>
#include <vector>

#include "rosbag2_cpp/converter_options.hpp"
#include "rosbag2_storage/bag_metadata.hpp"
#include "rosbag2_storage/default_storage_id.hpp"
#include "rosbag2_storage/metadata_io.hpp"
#include "rosbag2_storage/storage_filter.hpp"
#include "rosbag2_storage/storage_interfaces/base_read_interface.hpp"
#include "rosbag2_storage/storage_options.hpp"
#include "rosbag2_storage/topic_metadata.hpp"

#include "format_bag_metadata.hpp"
#include "pybind11.hpp"
#include "storage_values.hpp"
#include "value_properties.hpp"

namespace
{

using pybind11::literals::operator""_a;
using rosbag2_py::copy_views;
using rosbag2_py::native_snapshot;
using rosbag2_py::PyBagMetadata;
using rosbag2_py::PyTopicInformation;
using rosbag2_py::PyTopicMetadata;
using rosbag2_py::read_field;
using rosbag2_py::SharedValues;
using rosbag2_py::value_mutex;
using rosbag2_py::value_snapshot;
using rosbag2_py::value_snapshots;
using rosbag2_py::write_field;

template<typename ... Args>
auto update_qos(rclcpp::QoS & (rclcpp::QoS::* method)(Args ...))
{
  return [method](rclcpp::QoS & qos, Args ... args) -> rclcpp::QoS & {
           std::lock_guard<std::mutex> lock(value_mutex());
           return (qos.*method)(std::forward<Args>(args)...);
         };
}

pybind11::object to_rclpy_duration(std::chrono::nanoseconds duration)
{
  pybind11::object Duration = pybind11::module::import("rclpy.duration").attr("Duration");
  return Duration("nanoseconds"_a = duration.count());
}

std::chrono::nanoseconds from_rclpy_duration(const pybind11::object & duration)
{
  pybind11::int_ nanos = duration.attr("nanoseconds");
  return std::chrono::nanoseconds(nanos);
}

template<typename T>
pybind11::object to_rclpy_time(T time)
{
  pybind11::object Time = pybind11::module::import("rclpy.time").attr("Time");
  return Time(
    "nanoseconds"_a = std::chrono::duration_cast<std::chrono::nanoseconds>(
      time.time_since_epoch()).count());
}

std::chrono::time_point<std::chrono::high_resolution_clock> from_rclpy_time(
  const pybind11::object & time)
{
  pybind11::int_ nanos = time.attr("nanoseconds");
  return std::chrono::time_point<std::chrono::high_resolution_clock>(
    std::chrono::nanoseconds(nanos));
}

}  // namespace

PYBIND11_MODULE(_storage, m, pybind11::mod_gil_not_used()) {
  m.doc() = "Python wrapper of the rosbag2 utilities API";

  pybind11::class_<rosbag2_cpp::ConverterOptions>(m, "ConverterOptions")
  .def(
    pybind11::init<std::string, std::string>(),
    pybind11::arg("input_serialization_format") = "",
    pybind11::arg("output_serialization_format") = "")
  .def_property(
    "input_serialization_format",
    read_field(&rosbag2_cpp::ConverterOptions::input_serialization_format),
    write_field(&rosbag2_cpp::ConverterOptions::input_serialization_format))
  .def_property(
    "output_serialization_format",
    read_field(&rosbag2_cpp::ConverterOptions::output_serialization_format),
    write_field(&rosbag2_cpp::ConverterOptions::output_serialization_format));

  using KEY_VALUE_MAP = std::unordered_map<std::string, std::string>;
  pybind11::class_<rosbag2_storage::StorageOptions>(m, "StorageOptions")
  .def(
    pybind11::init(
      [](
        std::string uri,
        std::string storage_id,
        uint64_t max_bagfile_size,
        uint64_t max_bagfile_duration,
        uint64_t max_bag_files,
        uint64_t max_cache_size,
        uint32_t max_cache_duration,
        std::string storage_preset_profile,
        std::string storage_config_uri,
        bool snapshot_mode,
        int64_t start_time_ns,
        int64_t end_time_ns,
        KEY_VALUE_MAP custom_data)
      {
        return rosbag2_storage::StorageOptions{
          std::move(uri),
          std::move(storage_id),
          max_bagfile_size,
          max_bagfile_duration,
          max_bag_files,
          max_cache_size,
          max_cache_duration,
          std::move(storage_preset_profile),
          std::move(storage_config_uri),
          snapshot_mode,
          start_time_ns,
          end_time_ns,
          std::move(custom_data),
        };
      }),
    pybind11::arg("uri"),
    pybind11::arg("storage_id") = "",
    pybind11::arg("max_bagfile_size") = 0,
    pybind11::arg("max_bagfile_duration") = 0,
    pybind11::arg("max_bag_files") = 0,
    pybind11::arg("max_cache_size") = 0,
    pybind11::arg("max_cache_duration") = 0,
    pybind11::arg("storage_preset_profile") = "",
    pybind11::arg("storage_config_uri") = "",
    pybind11::arg("snapshot_mode") = false,
    pybind11::arg("start_time_ns") = -1,
    pybind11::arg("end_time_ns") = -1,
    pybind11::arg("custom_data") = KEY_VALUE_MAP{})
  .def_property(
    "uri",
    read_field(&rosbag2_storage::StorageOptions::uri),
    write_field(&rosbag2_storage::StorageOptions::uri))
  .def_property(
    "storage_id",
    read_field(&rosbag2_storage::StorageOptions::storage_id),
    write_field(&rosbag2_storage::StorageOptions::storage_id))
  .def_property(
    "max_bagfile_size",
    read_field(&rosbag2_storage::StorageOptions::max_bagfile_size),
    write_field(&rosbag2_storage::StorageOptions::max_bagfile_size))
  .def_property(
    "max_bagfile_duration",
    read_field(&rosbag2_storage::StorageOptions::max_bagfile_duration),
    write_field(&rosbag2_storage::StorageOptions::max_bagfile_duration))
  .def_property(
    "max_bag_files",
    read_field(&rosbag2_storage::StorageOptions::max_bag_files),
    write_field(&rosbag2_storage::StorageOptions::max_bag_files))
  .def_property(
    "max_cache_size",
    read_field(&rosbag2_storage::StorageOptions::max_cache_size),
    write_field(&rosbag2_storage::StorageOptions::max_cache_size))
  .def_property(
    "max_cache_duration",
    read_field(&rosbag2_storage::StorageOptions::max_cache_duration),
    write_field(&rosbag2_storage::StorageOptions::max_cache_duration))
  .def_property(
    "storage_preset_profile",
    read_field(&rosbag2_storage::StorageOptions::storage_preset_profile),
    write_field(&rosbag2_storage::StorageOptions::storage_preset_profile))
  .def_property(
    "storage_config_uri",
    read_field(&rosbag2_storage::StorageOptions::storage_config_uri),
    write_field(&rosbag2_storage::StorageOptions::storage_config_uri))
  .def_property(
    "snapshot_mode",
    read_field(&rosbag2_storage::StorageOptions::snapshot_mode),
    write_field(&rosbag2_storage::StorageOptions::snapshot_mode))
  .def_property(
    "start_time_ns",
    read_field(&rosbag2_storage::StorageOptions::start_time_ns),
    write_field(&rosbag2_storage::StorageOptions::start_time_ns))
  .def_property(
    "end_time_ns",
    read_field(&rosbag2_storage::StorageOptions::end_time_ns),
    write_field(&rosbag2_storage::StorageOptions::end_time_ns))
  .def_property(
    "custom_data",
    read_field(&rosbag2_storage::StorageOptions::custom_data),
    write_field(&rosbag2_storage::StorageOptions::custom_data));

  pybind11::class_<rosbag2_storage::StorageFilter>(m, "StorageFilter")
  .def(
    pybind11::init<
      std::vector<std::string>, std::vector<std::string>, std::vector<std::string>, std::string,
      std::vector<std::string>, std::vector<std::string>, std::vector<std::string>, std::string>(),
    pybind11::arg("topics") = std::vector<std::string>(),
    pybind11::arg("services_events") = std::vector<std::string>(),
    pybind11::arg("actions") = std::vector<std::string>(),
    pybind11::arg("regex") = "",
    pybind11::arg("exclude_topics") = std::vector<std::string>(),
    pybind11::arg("exclude_service_events") = std::vector<std::string>(),
    pybind11::arg("exclude_actions") = std::vector<std::string>(),
    pybind11::arg("regex_to_exclude") = "")
  .def_property(
    "topics",
    read_field(&rosbag2_storage::StorageFilter::topics),
    write_field(&rosbag2_storage::StorageFilter::topics))
  .def_property(
    "services_events",
    read_field(&rosbag2_storage::StorageFilter::services_events),
    write_field(&rosbag2_storage::StorageFilter::services_events))
  .def_property(
    "actions",
    read_field(&rosbag2_storage::StorageFilter::actions_interfaces),
    write_field(&rosbag2_storage::StorageFilter::actions_interfaces))
  .def_property(
    "regex",
    read_field(&rosbag2_storage::StorageFilter::regex),
    write_field(&rosbag2_storage::StorageFilter::regex))
  .def_property(
    "exclude_topics",
    read_field(&rosbag2_storage::StorageFilter::exclude_topics),
    write_field(&rosbag2_storage::StorageFilter::exclude_topics))
  .def_property(
    "exclude_service_events",
    read_field(&rosbag2_storage::StorageFilter::exclude_service_events),
    write_field(&rosbag2_storage::StorageFilter::exclude_service_events))
  .def_property(
    "exclude_actions",
    read_field(&rosbag2_storage::StorageFilter::exclude_actions_interfaces),
    write_field(&rosbag2_storage::StorageFilter::exclude_actions_interfaces))
  .def_property(
    "regex_to_exclude",
    read_field(&rosbag2_storage::StorageFilter::regex_to_exclude),
    write_field(&rosbag2_storage::StorageFilter::regex_to_exclude));

  pybind11::class_<rosbag2_storage::MessageDefinition>(m, "MessageDefinition")
  .def(
    pybind11::init<std::string, std::string, std::string, std::string>(),
    pybind11::arg("topic_type"),
    pybind11::arg("encoding"),
    pybind11::arg("encoded_message_definition"),
    pybind11::arg("type_hash"))
  .def_property(
    "topic_type", read_field(&rosbag2_storage::MessageDefinition::topic_type),
    write_field(&rosbag2_storage::MessageDefinition::topic_type))
  .def_property(
    "encoding", read_field(&rosbag2_storage::MessageDefinition::encoding),
    write_field(&rosbag2_storage::MessageDefinition::encoding))
  .def_property(
    "encoded_message_definition",
    read_field(&rosbag2_storage::MessageDefinition::encoded_message_definition),
    write_field(&rosbag2_storage::MessageDefinition::encoded_message_definition))
  .def_property(
    "type_hash", read_field(&rosbag2_storage::MessageDefinition::type_hash),
    write_field(&rosbag2_storage::MessageDefinition::type_hash));

  pybind11::enum_<rmw_qos_history_policy_t>(m, "rmw_qos_history_policy_t")
  .value("RMW_QOS_POLICY_HISTORY_SYSTEM_DEFAULT", RMW_QOS_POLICY_HISTORY_SYSTEM_DEFAULT)
  .value("RMW_QOS_POLICY_HISTORY_KEEP_LAST", RMW_QOS_POLICY_HISTORY_KEEP_LAST)
  .value("RMW_QOS_POLICY_HISTORY_KEEP_ALL", RMW_QOS_POLICY_HISTORY_KEEP_ALL)
  .value("RMW_QOS_POLICY_HISTORY_UNKNOWN", RMW_QOS_POLICY_HISTORY_UNKNOWN);

  pybind11::enum_<rmw_qos_reliability_policy_t>(m, "rmw_qos_reliability_policy_t")
  .value("RMW_QOS_POLICY_RELIABILITY_SYSTEM_DEFAULT", RMW_QOS_POLICY_RELIABILITY_SYSTEM_DEFAULT)
  .value("RMW_QOS_POLICY_RELIABILITY_RELIABLE", RMW_QOS_POLICY_RELIABILITY_RELIABLE)
  .value("RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT", RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT)
  .value("RMW_QOS_POLICY_RELIABILITY_UNKNOWN", RMW_QOS_POLICY_RELIABILITY_UNKNOWN);

  pybind11::enum_<rmw_qos_durability_policy_t>(m, "rmw_qos_durability_policy_t")
  .value("RMW_QOS_POLICY_DURABILITY_SYSTEM_DEFAULT", RMW_QOS_POLICY_DURABILITY_SYSTEM_DEFAULT)
  .value("RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL", RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL)
  .value("RMW_QOS_POLICY_DURABILITY_VOLATILE", RMW_QOS_POLICY_DURABILITY_VOLATILE)
  .value("RMW_QOS_POLICY_DURABILITY_UNKNOWN", RMW_QOS_POLICY_DURABILITY_UNKNOWN);


  pybind11::enum_<rmw_qos_liveliness_policy_t>(m, "rmw_qos_liveliness_policy_t")
  .value("RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT", RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT)
  .value("RMW_QOS_POLICY_LIVELINESS_AUTOMATIC", RMW_QOS_POLICY_LIVELINESS_AUTOMATIC)
  .value("RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC", RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC)
  .value("RMW_QOS_POLICY_LIVELINESS_UNKNOWN", RMW_QOS_POLICY_LIVELINESS_UNKNOWN);

  pybind11::class_<rclcpp::Duration>(m, "Duration")
  .def(
    pybind11::init<int32_t, uint32_t>(),
    pybind11::arg("seconds"),
    pybind11::arg("nanoseconds"));

  pybind11::class_<rclcpp::QoS, std::shared_ptr<rclcpp::QoS>>(m, "QoS")
  .def(
    pybind11::init<size_t>(),
    pybind11::arg("history_depth"))
  .def("keep_last", update_qos(&rclcpp::QoS::keep_last), pybind11::return_value_policy::reference)
  .def("keep_all", update_qos(&rclcpp::QoS::keep_all), pybind11::return_value_policy::reference)
  .def("reliable", update_qos(&rclcpp::QoS::reliable), pybind11::return_value_policy::reference)
  .def("best_effort", update_qos(&rclcpp::QoS::best_effort), pybind11::return_value_policy::reference)
  .def("durability_volatile", update_qos(&rclcpp::QoS::durability_volatile),
    pybind11::return_value_policy::reference)
  .def("transient_local", update_qos(&rclcpp::QoS::transient_local),
    pybind11::return_value_policy::reference)
  .def("history", update_qos(pybind11::overload_cast<rmw_qos_history_policy_t>(&rclcpp::QoS::history)),
    pybind11::return_value_policy::reference)
  .def(
    "reliability",
    update_qos(pybind11::overload_cast<rmw_qos_reliability_policy_t>(&rclcpp::QoS::reliability)),
    pybind11::return_value_policy::reference)
  .def("durability",
    update_qos(pybind11::overload_cast<rmw_qos_durability_policy_t>(&rclcpp::QoS::durability)),
    pybind11::return_value_policy::reference)
  .def("liveliness",
    update_qos(pybind11::overload_cast<rmw_qos_liveliness_policy_t>(&rclcpp::QoS::liveliness)),
    pybind11::return_value_policy::reference)
  .def("deadline", update_qos(pybind11::overload_cast<const rclcpp::Duration &>(&rclcpp::QoS::deadline)),
    pybind11::return_value_policy::reference)
  .def("lifespan", update_qos(pybind11::overload_cast<const rclcpp::Duration &>(&rclcpp::QoS::lifespan)),
    pybind11::return_value_policy::reference)
  .def(
    "liveliness_lease_duration",
    update_qos(
      pybind11::overload_cast<const rclcpp::Duration &>(&rclcpp::QoS::liveliness_lease_duration)),
    pybind11::return_value_policy::reference)
  .def(
    "avoid_ros_namespace_conventions",
    update_qos(pybind11::overload_cast<bool>(&rclcpp::QoS::avoid_ros_namespace_conventions)),
    pybind11::return_value_policy::reference);

  pybind11::class_<PyTopicMetadata, std::shared_ptr<PyTopicMetadata>>(m, "TopicMetadata")
  .def(
    pybind11::init([](
      uint16_t id, std::string name, std::string type, std::string serialization_format,
      const std::vector<pybind11::object> & offered_qos_profiles, std::string type_description_hash)
      {
        return PyTopicMetadata(rosbag2_storage::TopicMetadata{
          id, std::move(name), std::move(type), std::move(serialization_format),
          value_snapshots<rclcpp::QoS>(offered_qos_profiles), std::move(type_description_hash)});
      }),
    pybind11::arg("id"),
    pybind11::arg("name"),
    pybind11::arg("type"),
    pybind11::arg("serialization_format"),
    pybind11::arg("offered_qos_profiles") = std::vector<pybind11::object>(),
    pybind11::arg("type_description_hash") = "")
  .def_property(
    "id", read_field<PyTopicMetadata>(&PyTopicMetadata::id),
    write_field<PyTopicMetadata>(&PyTopicMetadata::id))
  .def_property(
    "name", read_field<PyTopicMetadata>(&PyTopicMetadata::name),
    write_field<PyTopicMetadata>(&PyTopicMetadata::name))
  .def_property(
    "type", read_field<PyTopicMetadata>(&PyTopicMetadata::type),
    write_field<PyTopicMetadata>(&PyTopicMetadata::type))
  .def_property(
    "serialization_format",
    read_field<PyTopicMetadata>(&PyTopicMetadata::serialization_format),
    write_field<PyTopicMetadata>(&PyTopicMetadata::serialization_format))
  .def_property(
    "offered_qos_profiles",
    read_field(&PyTopicMetadata::qos_profiles),
    [](PyTopicMetadata & self, const SharedValues<rclcpp::QoS> & values) {
      std::lock_guard<std::mutex> lock(value_mutex());
      self.qos_profiles = copy_views(values);
    })
  .def_property(
    "type_description_hash",
    read_field<PyTopicMetadata>(&PyTopicMetadata::type_description_hash),
    write_field<PyTopicMetadata>(&PyTopicMetadata::type_description_hash))
  .def("equals", [](const PyTopicMetadata & a, const PyTopicMetadata & b) {
      std::lock_guard<std::mutex> lock(value_mutex());
      return a.native() == b.native();
    });

  pybind11::class_<PyTopicInformation, std::shared_ptr<PyTopicInformation>>(m, "TopicInformation")
  .def(
    pybind11::init([](const PyTopicMetadata & topic, size_t count) {
      return PyTopicInformation({native_snapshot(topic), count});
    }),
    pybind11::arg("topic_metadata"),
    pybind11::arg("message_count"))
  .def_property(
    "topic_metadata",
    read_field(&PyTopicInformation::topic),
    [](PyTopicInformation & self, const PyTopicMetadata & value) {
      std::lock_guard<std::mutex> lock(value_mutex());
      *self.topic = PyTopicMetadata(value.native());
    })
  .def_property(
    "message_count", read_field(&PyTopicInformation::message_count),
    write_field(&PyTopicInformation::message_count));

  using FileInformation = rosbag2_storage::FileInformation;
  pybind11::class_<FileInformation, std::shared_ptr<FileInformation>>(m, "FileInformation")
  .def(
    pybind11::init(
      [](
        std::string path,
        pybind11::object starting_time,
        pybind11::object duration,
        size_t message_count)
      {
        return rosbag2_storage::FileInformation{
          path,
          from_rclpy_time(starting_time),
          from_rclpy_duration(duration),
          message_count
        };
      }),
    pybind11::arg("path"),
    pybind11::arg("starting_time"),
    pybind11::arg("duration"),
    pybind11::arg("message_count")
  )
  .def_property("path", read_field(&FileInformation::path), write_field(&FileInformation::path))
  .def_property(
    "starting_time",
    [](const rosbag2_storage::FileInformation & self) {
      return to_rclpy_time(read_field(&FileInformation::starting_time)(self));
    },
    [](rosbag2_storage::FileInformation & self, const pybind11::object & value) {
      write_field(&FileInformation::starting_time)(self, from_rclpy_time(value));
    })
  .def_property(
    "duration", read_field(&FileInformation::duration), write_field(&FileInformation::duration))
  .def_property(
    "message_count", read_field(&FileInformation::message_count),
    write_field(&FileInformation::message_count));

  pybind11::class_<PyBagMetadata>(m, "BagMetadata")
  .def(
    pybind11::init(
      [](
        int version,
        uint64_t bag_size,
        std::string storage_identifier,
        std::vector<std::string> relative_file_paths,
        const SharedValues<FileInformation> & files,
        pybind11::object duration,
        pybind11::object starting_time,
        uint64_t message_count,
        const SharedValues<PyTopicInformation> & topics_with_message_count,
        std::string compression_format,
        std::string compression_mode,
        std::unordered_map<std::string, std::string> custom_data,
        std::string ros_distro)
      {
        auto result = PyBagMetadata(rosbag2_storage::BagMetadata{
          version,
          bag_size,
          storage_identifier,
          relative_file_paths,
          {},
          from_rclpy_duration(duration),
          from_rclpy_time(starting_time),
          message_count,
          {},
          compression_format,
          compression_mode,
          custom_data,
          ros_distro,
        });
        std::lock_guard<std::mutex> lock(value_mutex());
        result.file_values = copy_views(files);
        result.topic_values = copy_views(topics_with_message_count);
        return result;
      }),
    pybind11::arg("version") = rosbag2_storage::BagMetadata{}.version,
    pybind11::arg("bag_size") = 0,
    pybind11::arg("storage_identifier") = "",
    pybind11::arg("relative_file_paths") = std::vector<std::string>(),
    pybind11::arg("files") = SharedValues<FileInformation>(),
    pybind11::arg("duration") = to_rclpy_duration(std::chrono::nanoseconds{0}),
    pybind11::arg("starting_time") = to_rclpy_time(
      std::chrono::time_point<std::chrono::high_resolution_clock>(std::chrono::nanoseconds{0})),
    pybind11::arg("message_count") = 0,
    pybind11::arg("topics_with_message_count") = SharedValues<PyTopicInformation>(),
    pybind11::arg("compression_format") = "",
    pybind11::arg("compression_mode") = "",
    pybind11::arg("custom_data") = std::unordered_map<std::string, std::string>(),
    pybind11::arg("ros_distro") = "")
  .def_property(
    "version", read_field<PyBagMetadata>(&PyBagMetadata::version),
    write_field<PyBagMetadata>(&PyBagMetadata::version))
  .def_property(
    "bag_size", read_field<PyBagMetadata>(&PyBagMetadata::bag_size),
    write_field<PyBagMetadata>(&PyBagMetadata::bag_size))
  .def_property(
    "storage_identifier", read_field<PyBagMetadata>(&PyBagMetadata::storage_identifier),
    write_field<PyBagMetadata>(&PyBagMetadata::storage_identifier))
  .def_property(
    "relative_file_paths", read_field<PyBagMetadata>(&PyBagMetadata::relative_file_paths),
    write_field<PyBagMetadata>(&PyBagMetadata::relative_file_paths))
  .def_property("files", read_field(&PyBagMetadata::file_values),
    [](PyBagMetadata & self, const SharedValues<FileInformation> & values) {
      std::lock_guard<std::mutex> lock(value_mutex());
      self.file_values = copy_views(values);
    })
  .def_property(
    "duration",
    [](const PyBagMetadata & self) {
      return to_rclpy_duration(read_field<PyBagMetadata>(&PyBagMetadata::duration)(self));
    },
    [](PyBagMetadata & self, const pybind11::object & value) {
      write_field<PyBagMetadata>(&PyBagMetadata::duration)(self, from_rclpy_duration(value));
    })
  .def_property(
    "starting_time",
    [](const PyBagMetadata & self) {
      return to_rclpy_time(read_field<PyBagMetadata>(&PyBagMetadata::starting_time)(self));
    },
    [](PyBagMetadata & self, const pybind11::object & value) {
      write_field<PyBagMetadata>(&PyBagMetadata::starting_time)(self, from_rclpy_time(value));
    })
  .def_property(
    "message_count", read_field<PyBagMetadata>(&PyBagMetadata::message_count),
    write_field<PyBagMetadata>(&PyBagMetadata::message_count))
  .def_property("topics_with_message_count", read_field(&PyBagMetadata::topic_values),
    [](PyBagMetadata & self, const SharedValues<PyTopicInformation> & values) {
      std::lock_guard<std::mutex> lock(value_mutex());
      self.topic_values = copy_views(values);
    })
  .def_property(
    "compression_format", read_field<PyBagMetadata>(&PyBagMetadata::compression_format),
    write_field<PyBagMetadata>(&PyBagMetadata::compression_format))
  .def_property(
    "compression_mode", read_field<PyBagMetadata>(&PyBagMetadata::compression_mode),
    write_field<PyBagMetadata>(&PyBagMetadata::compression_mode))
  .def_property(
    "custom_data", read_field<PyBagMetadata>(&PyBagMetadata::custom_data),
    write_field<PyBagMetadata>(&PyBagMetadata::custom_data))
  .def_property(
    "ros_distro", read_field<PyBagMetadata>(&PyBagMetadata::ros_distro),
    write_field<PyBagMetadata>(&PyBagMetadata::ros_distro))
  .def(
    "__repr__", [](const PyBagMetadata & metadata) {
      return rosbag2_py::format_bag_meta_data(native_snapshot(metadata));
    });

  pybind11::enum_<rosbag2_storage::ReadOrder::SortBy>(m, "ReadOrderSortBy")
  .value("ReceivedTimestamp", rosbag2_storage::ReadOrder::ReceivedTimestamp)
  .value("PublishedTimestamp", rosbag2_storage::ReadOrder::PublishedTimestamp)
  .value("File", rosbag2_storage::ReadOrder::File);

  pybind11::class_<rosbag2_storage::ReadOrder>(m, "ReadOrder")
  .def(
    pybind11::init<rosbag2_storage::ReadOrder::SortBy, bool>(),
    pybind11::arg("sort_by") = rosbag2_storage::ReadOrder{}.sort_by,
    pybind11::arg("reverse") = rosbag2_storage::ReadOrder{}.reverse)
  .def_property(
    "sort_by",
    read_field(&rosbag2_storage::ReadOrder::sort_by),
    write_field(&rosbag2_storage::ReadOrder::sort_by))
  .def_property(
    "reverse",
    read_field(&rosbag2_storage::ReadOrder::reverse),
    write_field(&rosbag2_storage::ReadOrder::reverse));

  m.def(
    "get_default_storage_id",
    &rosbag2_storage::get_default_storage_id,
    "Returns the default storage ID used when unspecified in StorageOptions");

  m.def(
    "to_rclcpp_qos_vector",
    &rosbag2_storage::to_rclcpp_qos_vector,
    "Convert QoS in YAML to std::vector<QoS>");

  m.def(
    "convert_rclcpp_qos_to_rclpy_qos",
    [](const rclcpp::QoS & qos) {
      const auto qos_input = value_snapshot(qos);
      pybind11::object rclcpy_qos = pybind11::module_::import("rclpy.qos");
      pybind11::object rclcpy_duration = pybind11::module_::import("rclpy.duration");
      pybind11::object duration_lifespan = rclcpy_qos.attr("Duration")(
        "seconds"_a = qos_input.lifespan().to_rmw_time().sec,
        "nanoseconds"_a = qos_input.lifespan().to_rmw_time().nsec);
      pybind11::object duration_deadline = rclcpy_qos.attr("Duration")(
        "seconds"_a = qos_input.deadline().to_rmw_time().sec,
        "nanoseconds"_a = qos_input.deadline().to_rmw_time().nsec);
      pybind11::object duration_liveliness_lease_duration = rclcpy_qos.attr("Duration")(
        "seconds"_a = qos_input.liveliness_lease_duration().to_rmw_time().sec,
        "nanoseconds"_a = qos_input.liveliness_lease_duration().to_rmw_time().nsec);

      pybind11::object qos_profile = rclcpy_qos.attr("QoSProfile")(
        "depth"_a = qos_input.depth(),
        "history"_a = static_cast<int>(qos_input.history()),
        "reliability"_a = static_cast<int>(qos_input.reliability()),
        "durability"_a = static_cast<int>(qos_input.durability()),
        "lifespan"_a = duration_lifespan,
        "deadline"_a = duration_deadline,
        "liveliness"_a = static_cast<int>(qos_input.liveliness()),
        "liveliness_lease_duration"_a = duration_liveliness_lease_duration,
        "avoid_ros_namespace_conventions"_a = qos_input.avoid_ros_namespace_conventions());

      return qos_profile;
    },
    "Converts rclcpp::QoS to rclpy.qos");

  pybind11::class_<rosbag2_storage::MetadataIo>(m, "MetadataIo")
  .def(pybind11::init<>())
  .def("write_metadata", [](rosbag2_storage::MetadataIo & io, const std::string & uri,
    const PyBagMetadata & metadata) {
      auto value = native_snapshot(metadata);
      std::lock_guard<std::mutex> lock(rosbag2_py::metadata_io_mutex());
      io.write_metadata(uri, value);
    })
  .def("read_metadata", [](rosbag2_storage::MetadataIo & io, const std::string & uri) {
      std::lock_guard<std::mutex> lock(rosbag2_py::metadata_io_mutex());
      return PyBagMetadata(io.read_metadata(uri));
    })
  .def("metadata_file_exists", &rosbag2_storage::MetadataIo::metadata_file_exists)
  .def("serialize_metadata", [](rosbag2_storage::MetadataIo & io, const PyBagMetadata & metadata) {
      return io.serialize_metadata(native_snapshot(metadata));
    })
  .def("deserialize_metadata", [](rosbag2_storage::MetadataIo & io, const std::string & input) {
      return PyBagMetadata(io.deserialize_metadata(input));
    })
  ;
}
