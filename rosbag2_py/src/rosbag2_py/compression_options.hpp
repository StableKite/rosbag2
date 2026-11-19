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

#ifndef ROSBAG2_PY__COMPRESSION_OPTIONS_HPP_
#define ROSBAG2_PY__COMPRESSION_OPTIONS_HPP_

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "rosbag2_compression/compression_options.hpp"

namespace rosbag2_py
{

// Python shares mutable options across threads. Native writers receive a copy.
class CompressionOptions
{
public:
  using Native = rosbag2_compression::CompressionOptions;

  CompressionOptions(
    std::string format, rosbag2_compression::CompressionMode mode, uint64_t queue_size,
    uint64_t threads)
  : options_{std::move(format), mode, queue_size, threads, std::nullopt} {}

  template<typename T>
  T get(T Native::* field) const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return options_.*field;
  }

  template<typename T>
  void set(T Native::* field, T value)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    options_.*field = std::move(value);
  }

  Native snapshot() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return options_;
  }

private:
  mutable std::mutex mutex_;
  Native options_;
};

}  // namespace rosbag2_py

#endif  // ROSBAG2_PY__COMPRESSION_OPTIONS_HPP_
