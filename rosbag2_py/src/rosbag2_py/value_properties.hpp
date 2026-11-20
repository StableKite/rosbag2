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

#ifndef ROSBAG2_PY__VALUE_PROPERTIES_HPP_
#define ROSBAG2_PY__VALUE_PROPERTIES_HPP_

#include <functional>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

#include "pybind11.hpp"

namespace rosbag2_py
{

// These value types are shared by several extension modules. TopicInformation
// also exposes a reference to its embedded TopicMetadata; both views must lock
// the same native data. Keep Python calls and ROS operations outside this lock.
inline std::mutex & value_mutex()
{
  return pybind11::get_or_create_shared_data<std::mutex>("rosbag2_py.value_mutex");
}

template<typename Value>
Value value_snapshot(const Value & value)
{
  std::lock_guard<std::mutex> lock(value_mutex());
  return value;
}

template<typename Value>
std::vector<Value> value_snapshots(const std::vector<pybind11::object> & values)
{
  std::vector<std::reference_wrapper<const Value>> native;
  native.reserve(values.size());
  for (const auto & value : values) {
    native.emplace_back(value.template cast<const Value &>());
  }
  std::lock_guard<std::mutex> lock(value_mutex());
  return std::vector<Value>(native.begin(), native.end());
}

template<typename Bound = void, typename Value, typename Member>
auto read_field(Member Value::* field)
{
  using Owner = std::conditional_t<std::is_void_v<Bound>, Value, Bound>;
  return [field](const Owner & value) -> Member {
           std::lock_guard<std::mutex> lock(value_mutex());
           return value.*field;
         };
}

template<typename Bound = void, typename Value, typename Member>
auto write_field(Member Value::* field)
{
  using Owner = std::conditional_t<std::is_void_v<Bound>, Value, Bound>;
  return [field](Owner & value, Member replacement) {
           std::lock_guard<std::mutex> lock(value_mutex());
           value.*field = std::move(replacement);
         };
}

}  // namespace rosbag2_py

#endif  // ROSBAG2_PY__VALUE_PROPERTIES_HPP_
