// Copyright 2026 Open Source Robotics Foundation, Inc.
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

#ifndef ROSBAG2_PY__STORAGE_VALUES_HPP_
#define ROSBAG2_PY__STORAGE_VALUES_HPP_

#include <memory>
#include <utility>
#include <vector>

#include "rosbag2_storage/bag_metadata.hpp"
#include "rosbag2_storage/topic_metadata.hpp"
#include "value_properties.hpp"

namespace rosbag2_py
{

inline std::mutex & metadata_io_mutex()
{
  return pybind11::get_or_create_shared_data<std::mutex>("rosbag2_py.metadata_io_mutex");
}

template<typename Value>
using SharedValues = std::vector<std::shared_ptr<Value>>;

template<typename Value, typename Input>
SharedValues<Value> share_values(std::vector<Input> values)
{
  SharedValues<Value> shared;
  shared.reserve(values.size());
  for (auto & value : values) {
    shared.push_back(std::make_shared<Value>(std::move(value)));
  }
  return shared;
}

// Python returns references to the elements of metadata fields. Hold each
// element independently so replacing the vector cannot invalidate those views.
// Read or convert the native values while holding value_mutex().
struct PyTopicMetadata : rosbag2_storage::TopicMetadata
{
  explicit PyTopicMetadata(rosbag2_storage::TopicMetadata value)
  : rosbag2_storage::TopicMetadata(std::move(value)),
    qos_profiles(share_values<rclcpp::QoS>(std::move(offered_qos_profiles)))
  {}

  rosbag2_storage::TopicMetadata native() const
  {
    rosbag2_storage::TopicMetadata value = *this;
    value.offered_qos_profiles.clear();
    value.offered_qos_profiles.reserve(qos_profiles.size());
    for (const auto & profile : qos_profiles) {
      value.offered_qos_profiles.push_back(*profile);
    }
    return value;
  }

  SharedValues<rclcpp::QoS> qos_profiles;
};

struct PyTopicInformation
{
  explicit PyTopicInformation(rosbag2_storage::TopicInformation value)
  : topic(std::make_shared<PyTopicMetadata>(std::move(value.topic_metadata))),
    message_count(value.message_count)
  {}

  rosbag2_storage::TopicInformation native() const
  {
    return {topic->native(), message_count};
  }

  std::shared_ptr<PyTopicMetadata> topic;
  size_t message_count;
};

struct PyBagMetadata : rosbag2_storage::BagMetadata
{
  explicit PyBagMetadata(rosbag2_storage::BagMetadata value)
  : rosbag2_storage::BagMetadata(std::move(value)),
    file_values(share_values<rosbag2_storage::FileInformation>(std::move(files))),
    topic_values(share_values<PyTopicInformation>(std::move(topics_with_message_count)))
  {}

  rosbag2_storage::BagMetadata native() const
  {
    rosbag2_storage::BagMetadata value = *this;
    value.files.clear();
    value.topics_with_message_count.clear();
    value.files.reserve(file_values.size());
    for (const auto & file : file_values) {
      value.files.push_back(*file);
    }
    value.topics_with_message_count.reserve(topic_values.size());
    for (const auto & topic : topic_values) {
      value.topics_with_message_count.push_back(topic->native());
    }
    return value;
  }

  SharedValues<rosbag2_storage::FileInformation> file_values;
  SharedValues<PyTopicInformation> topic_values;
};

template<typename Value>
auto native_snapshot(const Value & value)
{
  std::lock_guard<std::mutex> lock(value_mutex());
  return value.native();
}

template<typename Value>
SharedValues<Value> copy_views(const SharedValues<Value> & values)
{
  SharedValues<Value> copied;
  copied.reserve(values.size());
  for (const auto & value : values) {
    if (!value) {
      throw pybind11::type_error("metadata entries must not be None");
    }
    if constexpr (std::is_same_v<Value, PyTopicInformation>) {
      copied.push_back(std::make_shared<Value>(value->native()));
    } else {
      copied.push_back(std::make_shared<Value>(*value));
    }
  }
  return copied;
}

}  // namespace rosbag2_py

#endif  // ROSBAG2_PY__STORAGE_VALUES_HPP_
