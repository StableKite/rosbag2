// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
#include <atomic>
#include <csignal>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "keyboard_handler/keyboard_handler.hpp"
#include "rosbag2_storage/storage_options.hpp"
#include "rosbag2_storage/yaml.hpp"
#include "rosbag2_transport/bag_rewrite.hpp"
#include "rosbag2_transport/play_options.hpp"
#include "rosbag2_transport/player.hpp"
#include "rosbag2_transport/reader_writer_factory.hpp"
#include "rosbag2_transport/record_options.hpp"
#include "rosbag2_transport/recorder.hpp"

#include "rclcpp/executors/events_cbg_executor/events_cbg_executor.hpp"

#include "./pybind11.hpp"
#include "value_properties.hpp"

namespace py = pybind11;
typedef std::unordered_map<std::string, rclcpp::QoS> QoSMap;

namespace
{

using rosbag2_py::read_field;
using rosbag2_py::value_mutex;
using rosbag2_py::value_snapshot;
using rosbag2_py::value_snapshots;
using rosbag2_py::write_field;

class Arguments
{
public:
  explicit Arguments(const std::vector<std::string> & args)
  : arguments_(args)
  {
    std::for_each(
      arguments_.begin(), arguments_.end(),
      [this](const std::string & arg) {
        pointers_.push_back(arg.c_str());
      }
    );
    pointers_.push_back(nullptr);
  }

  const char ** argv()
  {
    return arguments_.empty() ? nullptr : pointers_.data();
  }

  [[nodiscard]] int argc() const
  {
    return static_cast<int>(arguments_.size());
  }

private:
  std::vector<std::string> arguments_;
  std::vector<const char *> pointers_;
};

rclcpp::QoS qos_from_handle(const py::handle source)
{
  const auto py_obj = source.attr("get_c_qos_profile")();
  const auto rmw_qos_profile = py_obj.cast<rmw_qos_profile_t>();
  const auto qos_init = rclcpp::QoSInitialization::from_rmw(rmw_qos_profile);
  return rclcpp::QoS{qos_init, rmw_qos_profile};
}

QoSMap qos_map_from_py_dict(const py::dict & dict)
{
  auto * copy = PyDict_Copy(dict.ptr());
  if (!copy) {
    throw py::error_already_set();
  }
  auto snapshot = py::reinterpret_steal<py::dict>(copy);
  QoSMap value;
  for (const auto & item : snapshot) {
    auto key = std::string(py::str(item.first));
    value.insert({key, qos_from_handle(item.second)});
  }
  return value;
}

/**
 * Simple wrapper subclass to provide nontrivial type conversions for python properties.
 */
template<class T>
struct OptionsWrapper : public T
{
public:
  void setDelay(double delay)
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    this->delay = rclcpp::Duration::from_nanoseconds(
      static_cast<rcl_duration_value_t>(RCUTILS_S_TO_NS(delay)));
  }

  double getPlaybackDuration() const
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    return RCUTILS_NS_TO_S(static_cast<double>(this->playback_duration.nanoseconds()));
  }

  void setPlaybackDuration(double playback_duration)
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    this->playback_duration = rclcpp::Duration::from_nanoseconds(
      static_cast<rcl_duration_value_t>(RCUTILS_S_TO_NS(playback_duration)));
  }

  double getDelay() const
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    return RCUTILS_NS_TO_S(static_cast<double>(this->delay.nanoseconds()));
  }

  void setStartOffset(double start_offset)
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    this->start_offset = static_cast<rcutils_time_point_value_t>(RCUTILS_S_TO_NS(start_offset));
  }

  double getStartOffset() const
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    return RCUTILS_NS_TO_S(static_cast<double>(this->start_offset));
  }

  void setPlaybackUntilTimestamp(int64_t playback_until_timestamp)
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    this->playback_until_timestamp =
      static_cast<rcutils_time_point_value_t>(playback_until_timestamp);
  }

  int64_t getPlaybackUntilTimestamp() const
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    return this->playback_until_timestamp;
  }

  void setTopicQoSProfileOverrides(const py::dict & overrides)
  {
    auto profiles = qos_map_from_py_dict(overrides);
    py::dict previous = overrides;
    std::lock_guard<std::mutex> lock(value_mutex());
    std::swap(py_dict, previous);
    this->topic_qos_profile_overrides = std::move(profiles);
  }

  py::dict getTopicQoSProfileOverrides() const
  {
    std::lock_guard<std::mutex> lock(value_mutex());
    return py_dict;
  }

  T snapshot() const
  {
    return value_snapshot(static_cast<const T &>(*this));
  }

  py::dict py_dict;
};
typedef OptionsWrapper<rosbag2_transport::PlayOptions> PlayOptions;
typedef OptionsWrapper<rosbag2_transport::RecordOptions> RecordOptions;

template<typename Binding, typename Value, typename Member>
void bind_field(py::class_<Binding> & binding, const char * name, Member Value::* field)
{
  binding.def_property(name, read_field<Binding>(field), write_field<Binding>(field));
}

rosbag2_transport::RecordOptions recording_options(RecordOptions & options)
{
  bool deprecated = false;
  auto snapshot = [&]() {
      std::lock_guard<std::mutex> lock(value_mutex());
      if (options.output_serialization_format.empty() && !options.rmw_serialization_format.empty()) {
        options.output_serialization_format = options.rmw_serialization_format;
        deprecated = true;
      }
      return static_cast<const rosbag2_transport::RecordOptions &>(options);
    }();
  if (deprecated && PyErr_WarnEx(PyExc_DeprecationWarning,
    "The rmw_serialization_format option is deprecated and will be removed in a "
    "future release.\nPlease use output_serialization_format instead.", 1) < 0)
  {
    throw py::error_already_set();
  }
  if (snapshot.output_serialization_format.empty()) {
    const char * format = rmw_get_serialization_format();
    if (!format) {
      throw std::runtime_error("RMW serialization format is unavailable");
    }
    snapshot.output_serialization_format = format;
    std::lock_guard<std::mutex> lock(value_mutex());
    if (options.output_serialization_format.empty()) {
      options.output_serialization_format = format;
    }
  }
  return snapshot;
}

class LegacySignals
{
public:
  LegacySignals()
  : lease_(mutex_, std::try_to_lock)
  {
    if (!lease_) {
      throw std::runtime_error("Another blocking rosbag2 operation is already handling signals");
    }
    received_.store(0, std::memory_order_relaxed);
    old_int_ = std::signal(SIGINT, receive);
    old_term_ = std::signal(SIGTERM, receive);
    if (old_int_ == SIG_ERR || old_term_ == SIG_ERR) {
      restore();
      throw std::runtime_error("Could not install rosbag2 signal handlers");
    }
  }

  ~LegacySignals()
  {
    if (lease_) {
      restore();
    }
  }

  static bool pending() noexcept
  {
    return received_.load(std::memory_order_relaxed) != 0;
  }

  void forward()
  {
    const auto sig = received_.load(std::memory_order_relaxed);
    const auto handler = sig == SIGINT ? old_int_ : old_term_;
    restore();
    lease_.unlock();
    if (sig != 0 && handler != SIG_ERR && handler != SIG_IGN && handler != SIG_DFL) {
      handler(sig);
    }
  }

private:
  using Handler = void (*)(int);

  static void receive(int sig) noexcept
  {
    received_.store(sig, std::memory_order_relaxed);
  }

  void restore() noexcept
  {
    if (old_int_ != SIG_ERR) {
      std::signal(SIGINT, old_int_);
    }
    if (old_term_ != SIG_ERR) {
      std::signal(SIGTERM, old_term_);
    }
  }

  inline static std::mutex mutex_;
  // The signal handler must not enter Python, allocate, or notify a condition variable.
  static_assert(std::atomic<int>::is_always_lock_free);
  inline static std::atomic<int> received_{0};
  std::unique_lock<std::mutex> lease_;
  Handler old_int_{SIG_ERR};
  Handler old_term_{SIG_ERR};
};

class LegacyStop
{
public:
  void reset()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    requested_ = false;
  }

  void request()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    requested_ = true;
    ready_.notify_all();
  }

  void wait()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!requested_ && !LegacySignals::pending()) {
      ready_.wait_for(lock, std::chrono::milliseconds(100));
    }
  }

private:
  std::mutex mutex_;
  std::condition_variable ready_;
  bool requested_{false};
};

}  // namespace

namespace rosbag2_py
{

class Player
{
public:
  // TODO(christophebedard): remove this constructor after a deprecation period
  // along with the `if (!player_)` checks in the methods
  explicit Player(const std::string & log_level = "info")
  {
    Arguments arguments({"--ros-args", "--log-level", log_level});
    rclcpp::init(arguments.argc(), arguments.argv());
    legacy_stop_.reset();
    // Intentionally not initializing player_ here, because default constructor is intended to be
    // used for the composable node only since it will call Player::Play() inside. Also it will
    // fail without specifying storage options with valid path to the bag file via ros args.
  }

  Player(
    const rosbag2_storage::StorageOptions & storage_options,
    const PlayOptions & play_options,
    const std::string & log_level,
    const std::string & node_name)
  : Player(
      {value_snapshot(storage_options)}, play_options.snapshot(), log_level, node_name) {}

  Player(
    const std::vector<rosbag2_storage::StorageOptions> & storage_options,
    const rosbag2_transport::PlayOptions & play_options,
    const std::string & log_level,
    const std::string & node_name)
  {
    Arguments arguments({"--ros-args", "--log-level", log_level});
    // Don't install signal handlers to keep signal handling simple in the Python layer
    rclcpp::init(arguments.argc(), arguments.argv(),
                 rclcpp::InitOptions(), rclcpp::SignalHandlerOptions::None);
    // If constructing the Player fails (e.g. the bag needs a converter plugin
    // that is not installed), the destructor never runs, so rclcpp::shutdown()
    // would otherwise only be called when the global Context is destroyed at
    // process exit.
    // Ensure the Context is shut down on the failure path so the program can exit
    // cleanly with the expected error code.
    try {
      player_ =
        std::make_shared<rosbag2_transport::Player>(storage_options, play_options, node_name);
      legacy_stop_.reset();
    } catch (...) {
      rclcpp::shutdown();
      throw;
    }
  }

  virtual ~Player()
  {
    stop_spin();
    rclcpp::shutdown();
  }

  void start_spin()
  {
    std::lock_guard<std::mutex> lock(spin_thread_mutex_);
    auto player = configured_player();
    if (exec_) {
      // We already have an executor spinning
      return;
    }
    exec_ = std::make_unique<rclcpp::executors::SingleThreadedExecutor>();
    exec_->add_node(player);
    spin_thread_ = std::thread(
      [this]() {
        exec_->spin();
      });
    // Wait for the executor to start spinning to avoid race conditions
    while (!exec_->is_spinning()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  void stop_spin()
  {
    std::lock_guard<std::mutex> lock(spin_thread_mutex_);
    if (exec_) {
      exec_->cancel();
      if (spin_thread_.joinable()) {
        spin_thread_.join();
      }
      exec_->remove_node(configured_player());
      exec_ = nullptr;
    }
  }

  void play()
  {
    auto player = configured_player();
    player->play();
  }

  bool wait_for_playback_to_start(double timeout = -1.0)
  {
    auto player = configured_player();
    return wait_for_async(
      timeout,
      [player](double timeout) {
        return player->wait_for_playback_to_start(std::chrono::duration<double>(timeout));
      });
  }

  bool wait_for_playback_to_start_exclusively(double timeout = -1.0)
  {
    return configured_player()->wait_for_playback_to_start(std::chrono::duration<double>(timeout));
  }

  bool wait_for_playback_to_finish(double timeout = -1.0)
  {
    auto player = configured_player();
    return wait_for_async(
      timeout,
      [player](double timeout) {
        return player->wait_for_playback_to_finish(std::chrono::duration<double>(timeout));
      });
  }

  bool wait_for_playback_to_finish_exclusively(double timeout = -1.0)
  {
    return configured_player()->wait_for_playback_to_finish(std::chrono::duration<double>(timeout));
  }

  void stop()
  {
    auto player = configured_player();
    player->stop();
  }

  void pause()
  {
    auto player = configured_player();
    player->pause();
  }

  void resume()
  {
    auto player = configured_player();
    player->resume();
  }

  [[nodiscard]] bool is_paused() const
  {
    auto player = configured_player();
    return player->is_paused();
  }

  [[nodiscard]] int64_t get_starting_time() const
  {
    auto player = configured_player();
    return player->get_starting_time();
  }

  [[nodiscard]] int64_t get_playback_duration() const
  {
    auto player = configured_player();
    return player->get_playback_duration();
  }

  bool play_next()
  {
    auto player = configured_player();
    std::lock_guard<std::mutex> lock(step_mutex_);
    return player->play_next();
  }

  size_t burst(size_t num_messages)
  {
    auto player = configured_player();
    std::lock_guard<std::mutex> lock(step_mutex_);
    return player->burst(num_messages);
  }

  void seek(int64_t time_point)
  {
    auto player = configured_player();
    player->seek(static_cast<rcutils_time_point_value_t>(time_point));
  }

  static void cancel()
  {
    if (PyErr_WarnEx(PyExc_DeprecationWarning,
      "Player.cancel() is deprecated. Please use Player.stop() instead.", 1) < 0)
    {
      throw py::error_already_set();
    }
    legacy_stop_.request();
  }

  // TODO(christophebedard): remove this method after a deprecation period
  void play(
    const rosbag2_storage::StorageOptions & storage_options,
    PlayOptions & play_options)
  {
    LegacySignals signals;
    replace_player(std::make_shared<rosbag2_transport::Player>(
      value_snapshot(storage_options), play_options.snapshot()));
    play_impl(signals);
  }

  // TODO(christophebedard): remove this method after a deprecation period
  void play(
    const std::vector<py::object> & storage_options,
    PlayOptions & play_options)
  {
    LegacySignals signals;
    replace_player(std::make_shared<rosbag2_transport::Player>(
      value_snapshots<rosbag2_storage::StorageOptions>(storage_options), play_options.snapshot()));
    play_impl(signals);
  }

  // TODO(christophebedard): remove this method after a deprecation period
  void burst(
    const rosbag2_storage::StorageOptions & storage_options,
    PlayOptions & play_options,
    size_t num_messages)
  {
    LegacySignals signals;
    replace_player(std::make_shared<rosbag2_transport::Player>(
      value_snapshot(storage_options), play_options.snapshot()));
    play_impl(signals, true, num_messages);
  }

protected:
  std::shared_ptr<rosbag2_transport::Player> configured_player() const
  {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!player_) {
      throw std::runtime_error("Player is not initialized. Please use constructor with "
        "storage and play options.");
    }
    return player_;
  }

  void replace_player(std::shared_ptr<rosbag2_transport::Player> player)
  {
    std::scoped_lock lock(spin_thread_mutex_, node_mutex_);
    if (exec_) {
      throw std::runtime_error("Stop the Player executor before replacing its configuration");
    }
    player_.swap(player);
  }

  bool wait_for_async(double timeout, const std::function<bool(double)> & wait_for_function)
  {
    const double wait_period =
      (timeout < 0.0) ? SIGNAL_CHECK_INTERVAL : std::min(timeout, SIGNAL_CHECK_INTERVAL);

    // We need to release the Global Interpreter Lock (GIL) to allow other Python threads to
    // proceed. However, the GIL release doesn't result in Python checking for signals while
    // we're in C++. We will wait for a short period of time and then check for signals in the loop.
    // If any signals (Ctrl+C/SIGINT or SIGTERM) have been received, we will propagate them to
    // Python.
    bool finished = false;
    while (!finished) {
      {
        // Release the Global Interpreter Lock (GIL) to allow other Python threads to proceed
        py::gil_scoped_release release;
        finished = wait_for_function(wait_period);
      }
      // The GIL release doesn't result in Python checking for signals while we're in C++, so we can
      // do it here to simplify the Python layer: just periodically check for signals while waiting
      // Wait for a short period of time
      {
        py::gil_scoped_acquire gil;
        // If a signal (Ctrl+C/SIGINT or SIGTERM) has been received, propagate it to Python
        if (PyErr_CheckSignals() != 0) {
          throw py::error_already_set();
        }
      }
      // If we are not done yet and have a non-infinite timeout value
      if (!finished && timeout > 0.0) {
        // Decrement the timeout that's left
        timeout -= wait_period;
        // If we waited all the way to the timeout (and aren't done), return false
        if (timeout <= 0.0) {
          return false;
        }
      }
    }
    return finished;
  }

  void play_impl(LegacySignals & signals, bool burst = false, size_t burst_num_messages = 0)
  {
    auto player = configured_player();
    std::thread wait_for_exit;
    std::exception_ptr failure;
    std::exception_ptr stop_failure;
    try {
      start_spin();
      player->play();
      wait_for_exit = std::thread(
        [player, &stop_failure]() {
          legacy_stop_.wait();
          try {
            player->stop();
          } catch (...) {
            stop_failure = std::current_exception();
          }
        });
      {
        py::gil_scoped_release release;
        if (burst) {
          std::lock_guard<std::mutex> lock(step_mutex_);
          player->burst(burst_num_messages);
        }
        player->wait_for_playback_to_finish();
      }
    } catch (...) {
      failure = std::current_exception();
    }
    legacy_stop_.request();
    if (wait_for_exit.joinable()) {
      wait_for_exit.join();
    }
    legacy_stop_.reset();
    stop_spin();
    signals.forward();
    if (failure || stop_failure) {
      std::rethrow_exception(failure ? failure : stop_failure);
    }
  }

  inline static LegacyStop legacy_stop_;
  static constexpr double SIGNAL_CHECK_INTERVAL = 0.1;

  std::shared_ptr<rosbag2_transport::Player> player_;
  mutable std::mutex node_mutex_;
  std::mutex step_mutex_;
  std::mutex spin_thread_mutex_;
  std::unique_ptr<rclcpp::executors::SingleThreadedExecutor> exec_{nullptr};
  std::thread spin_thread_;
};

class Recorder
{
public:
  // TODO(christophebedard): remove this constructor after a deprecation period
  // along with the `if (!recorder_)` checks in the methods
  explicit Recorder(const std::string & log_level = "info")
  {
    Arguments arguments({"--ros-args", "--log-level", log_level});
    rclcpp::init(arguments.argc(), arguments.argv());
    legacy_stop_.reset();
    // Intentionally not initializing recorder_ here, because default constructor is intended to be
    // used for the composable node only since it will call Recorder::record() inside.
  }

  Recorder(
    const rosbag2_storage::StorageOptions & storage_options,
    RecordOptions & record_options,
    const std::string & log_level,
    const std::string & node_name)
  {
    Arguments arguments({"--ros-args", "--log-level", log_level});
    // Don't install signal handlers to keep signal handling simple in the Python layer
    rclcpp::init(arguments.argc(), arguments.argv(),
                 rclcpp::InitOptions(), rclcpp::SignalHandlerOptions::None);

    // See the Player constructor: if constructing the Recorder throws, shut the
    // Context down here so it is not left to be destroyed at process exit.
    try {
      auto recording = recording_options(record_options);
      auto writer = rosbag2_transport::ReaderWriterFactory::make_writer(recording);

      recorder_ = std::make_shared<rosbag2_transport::Recorder>(
        std::move(writer), value_snapshot(storage_options), recording, node_name);
      legacy_stop_.reset();
    } catch (...) {
      rclcpp::shutdown();
      throw;
    }
  }

  virtual ~Recorder()
  {
    stop_spin();
    rclcpp::shutdown();
  }

  void start_spin()
  {
    std::lock_guard<std::mutex> lock(spin_thread_mutex_);
    auto recorder = configured_recorder();
    if (exec_) {
      // We already have an executor spinning
      return;
    }
    exec_ = std::make_unique<rclcpp::executors::EventsCBGExecutor>(
      rclcpp::ExecutorOptions(), 1);
    exec_->add_node(recorder);
    spin_thread_ = std::thread(
      [this]() {
        exec_->spin();
      });
    // Wait for the executor to start spinning to avoid race conditions
    while (!exec_->is_spinning()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  void stop_spin()
  {
    std::lock_guard<std::mutex> lock(spin_thread_mutex_);
    if (exec_) {
      exec_->cancel();
      if (spin_thread_.joinable()) {
        spin_thread_.join();
      }
      exec_->remove_node(configured_recorder());
      exec_ = nullptr;
    }
  }

  void record()
  {
    auto recorder = configured_recorder();
    recorder->record();
  }

  void stop()
  {
    auto recorder = configured_recorder();
    recorder->stop();
  }

  void pause()
  {
    auto recorder = configured_recorder();
    recorder->pause();
  }

  void resume()
  {
    auto recorder = configured_recorder();
    recorder->resume();
  }

  bool is_paused()
  {
    auto recorder = configured_recorder();
    return recorder->is_paused();
  }

  // TODO(christophebedard): remove this method after a deprecation period
  void record(
    const rosbag2_storage::StorageOptions & storage_options,
    RecordOptions & record_options,
    const std::string & node_name)
  {
    LegacySignals signals;
    auto recording = recording_options(record_options);
    auto writer = rosbag2_transport::ReaderWriterFactory::make_writer(recording);

    replace_recorder(std::make_shared<rosbag2_transport::Recorder>(
      std::move(writer), value_snapshot(storage_options), recording, node_name));

    record_impl(signals);
  }

  void record_impl(LegacySignals & signals)
  {
    auto recorder = configured_recorder();
    std::exception_ptr failure;
    try {
      recorder->record();
      // Run exec->spin() in a separate thread, because we need to call exec->cancel() after
      // recorder->stop() to be able to send notifications about bag split and close.
      start_spin();
      {
        // Release the GIL for long-running record, so that calling Python code
        // can use other threads
        py::gil_scoped_release release;
        legacy_stop_.wait();
        recorder->stop();
      }
    } catch (...) {
      failure = std::current_exception();
    }
    legacy_stop_.reset();
    stop_spin();
    signals.forward();
    if (failure) {
      std::rethrow_exception(failure);
    }
  }

  static void cancel()
  {
    if (PyErr_WarnEx(PyExc_DeprecationWarning,
      "Recorder.cancel() is deprecated. Please use Recorder.stop() instead.", 1) < 0)
    {
      throw py::error_already_set();
    }
    legacy_stop_.request();
  }

protected:
  std::shared_ptr<rosbag2_transport::Recorder> configured_recorder() const
  {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!recorder_) {
      throw std::runtime_error("Recorder is not initialized. Please use constructor with "
        "storage and record options.");
    }
    return recorder_;
  }

  void replace_recorder(std::shared_ptr<rosbag2_transport::Recorder> recorder)
  {
    std::scoped_lock lock(spin_thread_mutex_, node_mutex_);
    if (exec_) {
      throw std::runtime_error("Stop the Recorder executor before replacing its configuration");
    }
    recorder_.swap(recorder);
  }

  inline static LegacyStop legacy_stop_;

  std::shared_ptr<rosbag2_transport::Recorder> recorder_;
  mutable std::mutex node_mutex_;
  std::mutex spin_thread_mutex_;
  std::unique_ptr<rclcpp::executors::EventsCBGExecutor> exec_{nullptr};
  std::thread spin_thread_;
};

// Return a RecordOptions struct with defaults set for rewriting bags.
rosbag2_transport::RecordOptions bag_rewrite_default_record_options()
{
  rosbag2_transport::RecordOptions options{};
  // We never want to drop messages when converting bags, so set the compression queue size to 0
  // (unbounded).
  options.compression_queue_size = 0;
  return options;
}

/// \brief Simple wrapper to read the input and output config YAML files into structs
/// and call rosbag2_transport::bag_rewrite().
void bag_rewrite(
  const std::vector<py::object> & input_options,
  const std::string & input_config_file,
  const std::string & output_config_file)
{
  // Validate that only one input source is provided
  bool has_input_options = !input_options.empty();
  bool has_input_config = !input_config_file.empty();

  if (!has_input_options && !has_input_config) {
    throw std::runtime_error(
      "Either input_options vector or input_config_file must be provided.");
  }

  if (has_input_options && has_input_config) {
    throw std::runtime_error("Exactly one input source must be provided: provide either"
                             " input_options or input_config_file, but not both.");
  }

  auto effective_input_options = value_snapshots<rosbag2_storage::StorageOptions>(input_options);

  // Parse input options from YAML if provided
  if (has_input_config) {
    effective_input_options.clear();
    YAML::Node input_yaml = YAML::LoadFile(input_config_file);
    auto input_nodes = input_yaml["input_bags"];
    if (!input_nodes) {
      throw std::runtime_error("Input bag config YAML file must have top-level key 'input_bags'");
    }
    if (!input_nodes.IsSequence()) {
      throw std::runtime_error(
        "Top-level key 'input_bags' must contain a list of StorageOptions dicts.");
    }

    for (const auto & bag_node : input_nodes) {
      rosbag2_storage::StorageOptions storage_options{};
      YAML::convert<rosbag2_storage::StorageOptions>::decode(bag_node, storage_options);

      if (storage_options.uri.empty()) {
        throw std::runtime_error("Input bag StorageOptions must specify a non-empty 'uri'.");
      }
      if (!std::filesystem::exists(storage_options.uri)) {
        throw std::runtime_error("Bag path '" + storage_options.uri + "' does not exist!");
      }

      effective_input_options.push_back(storage_options);
    }
  }

  // Parse output options from YAML
  YAML::Node yaml_file = YAML::LoadFile(output_config_file);
  auto bag_nodes = yaml_file["output_bags"];
  if (!bag_nodes) {
    throw std::runtime_error("Output bag config YAML file must have top-level key 'output_bags'");
  }
  if (!bag_nodes.IsSequence()) {
    throw std::runtime_error(
            "Top-level key 'output_bags' must contain a list of "
            "StorageOptions/RecordOptions dicts.");
  }

  std::vector<
    std::pair<rosbag2_storage::StorageOptions, rosbag2_transport::RecordOptions>> output_options;
  for (const auto & bag_node : bag_nodes) {
    rosbag2_storage::StorageOptions storage_options{};
    YAML::convert<rosbag2_storage::StorageOptions>::decode(bag_node, storage_options);
    rosbag2_transport::RecordOptions record_options = bag_rewrite_default_record_options();
    YAML::convert<rosbag2_transport::RecordOptions>::decode(bag_node, record_options);
    output_options.push_back(std::make_pair(storage_options, record_options));
  }
  rosbag2_transport::bag_rewrite(effective_input_options, output_options);
}

}  // namespace rosbag2_py

PYBIND11_MODULE(_transport, m, py::mod_gil_not_used()) {
  m.doc() = "Python wrapper of the rosbag2_transport API";

  // NOTE: it is non-trivial to add a constructor for PlayOptions and RecordOptions
  // because the rclcpp::QoS <-> rclpy.qos.QoS Profile conversion cannot be done by builtins.
  // It is possible, but the code is much longer and harder to maintain, requiring duplicating
  // the names of the members multiple times, as well as the default values from the struct
  // definitions.

  py::class_<PlayOptions> play_options(m, "PlayOptions");
  play_options
  .def(py::init<>())
  .def_property(
    "topic_qos_profile_overrides",
    &PlayOptions::getTopicQoSProfileOverrides,
    &PlayOptions::setTopicQoSProfileOverrides)
  .def_property(
    "delay",
    &PlayOptions::getDelay,
    &PlayOptions::setDelay)
  .def_property(
    "playback_duration",
    &PlayOptions::getPlaybackDuration,
    &PlayOptions::setPlaybackDuration)
  .def_property(
    "start_offset",
    &PlayOptions::getStartOffset,
    &PlayOptions::setStartOffset)
  .def_property(
    "playback_until_timestamp",
    &PlayOptions::getPlaybackUntilTimestamp,
    &PlayOptions::setPlaybackUntilTimestamp)
  ;
  bind_field(play_options, "read_ahead_queue_size", &PlayOptions::read_ahead_queue_size);
  bind_field(play_options, "node_prefix", &PlayOptions::node_prefix);
  bind_field(play_options, "rate", &PlayOptions::rate);
  bind_field(play_options, "topics_to_filter", &PlayOptions::topics_to_filter);
  bind_field(play_options, "services_to_filter", &PlayOptions::services_to_filter);
  bind_field(play_options, "actions_to_filter", &PlayOptions::actions_to_filter);
  bind_field(play_options, "regex_to_filter", &PlayOptions::regex_to_filter);
  bind_field(play_options, "exclude_regex_to_filter", &PlayOptions::exclude_regex_to_filter);
  bind_field(play_options, "exclude_topics_to_filter", &PlayOptions::exclude_topics_to_filter);
  bind_field(play_options, "exclude_service_events_to_filter",
    &PlayOptions::exclude_services_to_filter);
  bind_field(play_options, "exclude_actions_to_filter", &PlayOptions::exclude_actions_to_filter);
  bind_field(play_options, "loop", &PlayOptions::loop);
  bind_field(play_options, "topic_remapping_options", &PlayOptions::topic_remapping_options);
  bind_field(play_options, "clock_publish_frequency", &PlayOptions::clock_publish_frequency);
  bind_field(play_options, "clock_publish_on_topic_publish",
    &PlayOptions::clock_publish_on_topic_publish);
  bind_field(play_options, "clock_topics", &PlayOptions::clock_trigger_topics);
  bind_field(play_options, "disable_keyboard_controls", &PlayOptions::disable_keyboard_controls);
  bind_field(play_options, "start_paused", &PlayOptions::start_paused);
  bind_field(play_options, "progress_bar_update_rate", &PlayOptions::progress_bar_update_rate);
  bind_field(play_options, "progress_bar_separation_lines",
    &PlayOptions::progress_bar_separation_lines);
  bind_field(play_options, "wait_acked_timeout", &PlayOptions::wait_acked_timeout);
  bind_field(play_options, "disable_loan_message", &PlayOptions::disable_loan_message);
  bind_field(play_options, "publish_service_requests", &PlayOptions::publish_service_requests);
  bind_field(play_options, "send_actions_as_client", &PlayOptions::send_actions_as_client);
  bind_field(play_options, "service_requests_source", &PlayOptions::service_requests_source);
  bind_field(play_options, "message_order", &PlayOptions::message_order);

  py::enum_<rosbag2_transport::ServiceRequestsSource>(m, "ServiceRequestsSource")
  .value("SERVICE_INTROSPECTION", rosbag2_transport::ServiceRequestsSource::SERVER_INTROSPECTION)
  .value("CLIENT_INTROSPECTION", rosbag2_transport::ServiceRequestsSource::CLIENT_INTROSPECTION)
  ;

  py::enum_<rosbag2_transport::MessageOrder>(m, "MessageOrder")
  .value("RECEIVED_TIMESTAMP", rosbag2_transport::MessageOrder::RECEIVED_TIMESTAMP)
  .value("SENT_TIMESTAMP", rosbag2_transport::MessageOrder::SENT_TIMESTAMP)
  ;

  py::class_<RecordOptions> record_options(m, "RecordOptions");
  record_options
  .def(py::init<>())
  .def_property(
    "topic_qos_profile_overrides",
    &RecordOptions::getTopicQoSProfileOverrides,
    &RecordOptions::setTopicQoSProfileOverrides)
  ;
  bind_field(record_options, "all_topics", &RecordOptions::all_topics);
  bind_field(record_options, "is_discovery_disabled", &RecordOptions::is_discovery_disabled);
  bind_field(record_options, "topics", &RecordOptions::topics);
  bind_field(record_options, "static_topics_uri", &RecordOptions::static_topics_uri);
  bind_field(record_options, "topic_types", &RecordOptions::topic_types);
  bind_field(record_options, "exclude_topic_types", &RecordOptions::exclude_topic_types);
  bind_field(record_options, "rmw_serialization_format", &RecordOptions::rmw_serialization_format);
  bind_field(record_options, "input_serialization_format",
    &RecordOptions::input_serialization_format);
  bind_field(record_options, "output_serialization_format",
    &RecordOptions::output_serialization_format);
  bind_field(record_options, "topic_polling_interval", &RecordOptions::topic_polling_interval);
  bind_field(record_options, "regex", &RecordOptions::regex);
  bind_field(record_options, "exclude_regex", &RecordOptions::exclude_regex);
  bind_field(record_options, "exclude_topics", &RecordOptions::exclude_topics);
  bind_field(record_options, "exclude_service_events", &RecordOptions::exclude_service_events);
  bind_field(record_options, "node_prefix", &RecordOptions::node_prefix);
  bind_field(record_options, "compression_mode", &RecordOptions::compression_mode);
  bind_field(record_options, "compression_format", &RecordOptions::compression_format);
  bind_field(record_options, "compression_queue_size", &RecordOptions::compression_queue_size);
  bind_field(record_options, "compression_threads", &RecordOptions::compression_threads);
  bind_field(record_options, "compression_threads_priority",
    &RecordOptions::compression_threads_priority);
  bind_field(record_options, "include_hidden_topics", &RecordOptions::include_hidden_topics);
  bind_field(record_options, "include_unpublished_topics",
    &RecordOptions::include_unpublished_topics);
  bind_field(record_options, "start_paused", &RecordOptions::start_paused);
  bind_field(record_options, "ignore_leaf_topics", &RecordOptions::ignore_leaf_topics);
  bind_field(record_options, "use_sim_time", &RecordOptions::use_sim_time);
  bind_field(record_options, "services", &RecordOptions::services);
  bind_field(record_options, "all_services", &RecordOptions::all_services);
  bind_field(record_options, "disable_keyboard_controls",
    &RecordOptions::disable_keyboard_controls);
  bind_field(record_options, "actions", &RecordOptions::actions);
  bind_field(record_options, "all_actions", &RecordOptions::all_actions);
  bind_field(record_options, "exclude_actions", &RecordOptions::exclude_actions);
  bind_field(record_options, "statistics_max_publishing_rate",
    &RecordOptions::statistics_max_publishing_rate);
  bind_field(record_options, "repeat_transient_local_messages",
    &RecordOptions::repeat_transient_local_messages);
  bind_field(record_options, "repeat_all_transient_local_depth",
    &RecordOptions::repeat_all_transient_local_depth);

  py::class_<rosbag2_py::Player>(m, "Player")
  // TODO(christophebedard): remove this constructor after a deprecation period
  // Deprecated default constructor
  .def(py::init([]()
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Player() is deprecated. Use the constructor with "
          "full configuration parameters instead.", 1);
      return new rosbag2_py::Player();
    }), "Deprecated: Use constructor with full options.")

  // TODO(christophebedard): remove this constructor after a deprecation period
  // Deprecated constructor with log_level
  .def(py::init([](const std::string & log_level)
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Player(log_level) is deprecated. Use the "
          "constructor with full configuration parameters instead.", 1);
      return new rosbag2_py::Player(log_level);
    }), py::arg("log_level"), "Deprecated: Use constructor with full options.")

  // Recommended constructor with storage and play options
  .def(py::init<const rosbag2_storage::StorageOptions &, const PlayOptions &,
    const std::string &, const std::string &>(),
    py::arg("storage_options"),
    py::arg("play_options"),
    py::arg("log_level") = "info",
    py::arg("node_name") = "rosbag2_player",
    R"pbdoc(
      Initialize a Player with complete configuration.

      Args:
          storage_options (StorageOptions): Configuration for storage backend (e.g., URI, format).
          play_options (PlayOptions): Options for playback (e.g., topics, QoS settings).
          log_level (str, optional): Logging level, defaults to 'info'.
          node_name (str, optional): Name of the player node, defaults to 'rosbag2_player'.
    )pbdoc")

  // Recommended constructor with multiple storage options
  .def(py::init([](const std::vector<py::object> & storage_options,
    const PlayOptions & play_options, const std::string & log_level, const std::string & node_name) {
      return std::make_unique<rosbag2_py::Player>(
        value_snapshots<rosbag2_storage::StorageOptions>(storage_options),
        play_options.snapshot(), log_level, node_name);
    }),
    py::arg("storage_options"),
    py::arg("play_options"),
    py::arg("log_level") = "info",
    py::arg("node_name") = "rosbag2_player",
    R"pbdoc(
      Initialize a Player with multiple storage options.

      Args:
          storage_options (List[StorageOptions]): List of storage configurations.
          play_options (PlayOptions): Options for playback (e.g., topics, QoS settings).
          log_level (str, optional): Logging level, defaults to 'info'.
          node_name (str, optional): Name of the player node, defaults to 'rosbag2_player'.
    )pbdoc")

  .def("start_spin", &rosbag2_py::Player::start_spin,
    R"pbdoc(
      Create and start a thread with an executor that spins the player node.

      Waits for the executor to start spinning before returning.
    )pbdoc")

  .def("stop_spin", &rosbag2_py::Player::stop_spin,
    R"pbdoc(
      Stop the thread that spins the player node, if there is one.

      This is automatically called by the destructor, if needed.
    )pbdoc")

  // Recommended play method
  .def("play", py::overload_cast<>(&rosbag2_py::Player::play),
    R"pbdoc(
      Start playback based on the internal Player configuration.

      This is the preferred method for starting playback.
      It is equivalent to the rosbag2_transport::Player::player() method.
      All parameters should be configured via the constructor.
    )pbdoc")

  .def("wait_for_playback_to_start",
    &rosbag2_py::Player::wait_for_playback_to_start,
    py::arg("timeout") = -1.0,
    R"pbdoc(
      Releasing Global Interpreter Lock (GIL) to allow other Python threads to proceed then wait
      for the playback to start and for the message queue to be filled. Also periodically checks for
      signals (Ctrl+C/SIGINT or SIGTERM) and propagates them to Python.

      Args:
          timeout (float): Maximum time to wait in seconds. Default is -1 (wait indefinitely).
      Returns:
          bool: True if playback started successfully, False if timed out.
    )pbdoc")

  .def("wait_for_playback_to_start_exclusively",
    &rosbag2_py::Player::wait_for_playback_to_start_exclusively,
    py::arg("timeout") = -1.0,
    R"pbdoc(
      Wait for the playback to start exclusively without releasing Global Interpreter Lock (GIL).

      Args:
          timeout (float): Maximum time to wait in seconds. Default is -1 (wait indefinitely).
      Returns:
          bool: True if playback started successfully, False if timed out.
    )pbdoc")

  .def("wait_for_playback_to_finish",
    &rosbag2_py::Player::wait_for_playback_to_finish,
    py::arg("timeout") = -1.0,
    R"pbdoc(
      Releasing Global Interpreter Lock (GIL) to allow other Python threads to proceed then wait
      for the playback to finish.  Also periodically checks for signals (Ctrl+C/SIGINT or SIGTERM)
      and propagates them to Python.

      Args:
          timeout (float): Maximum time to wait in seconds. Default is -1 (wait indefinitely).
      Returns:
          bool: True if playback finished successfully, False if timed out.
    )pbdoc")

  .def("wait_for_playback_to_finish_exclusively",
    &rosbag2_py::Player::wait_for_playback_to_finish_exclusively,
    py::arg("timeout") = -1.0,
    R"pbdoc(
      Wait for the playback to finish exclusively without releasing Global Interpreter Lock (GIL).

      Args:
          timeout (float): Maximum time to wait in seconds. Default is -1 (wait indefinitely).
      Returns:
          bool: True if playback finished successfully, False if timed out.
    )pbdoc")

  .def(
    "stop",
    &rosbag2_py::Player::stop,
    "Unpause if in pause mode, stop playback and exit from play.")

  .def(
    "pause",
    &rosbag2_py::Player::pause,
    "Pause the flow of time for playback.")

  .def(
    "resume",
    &rosbag2_py::Player::resume,
    "Start the flow of time for playback.")

  .def(
    "is_paused",
    &rosbag2_py::Player::is_paused,
    "Whether the playback is currently paused.")

  .def("get_starting_time",
    &rosbag2_py::Player::get_starting_time,
    R"pbdoc(
      Get the starting time of the playback.

      Returns:
          int: The timestamp of the first message in nanoseconds.
    )pbdoc")

  .def("get_playback_duration",
    &rosbag2_py::Player::get_playback_duration,
    R"pbdoc(
      Get the total duration of the playback.

      Returns:
          int: The total duration of the playback in nanoseconds.
    )pbdoc")

  .def("play_next", &rosbag2_py::Player::play_next,
    R"pbdoc(
      Playing next message from queue when in pause.

      Returns:
          bool: True if a message was played, False if no more messages are available.
    )pbdoc")

  .def("seek", &rosbag2_py::Player::seek, py::arg("time_point"),
    R"pbdoc(
      Advance player to the message with closest timestamp >= time_point.

      Args:
          time_point (int): Time point in ROS playback timeline, in nanoseconds.
    )pbdoc")

  // TODO(christophebedard): remove this method after a deprecation period
  // Deprecated play method with storage and play options
  .def("play",
    [](rosbag2_py::Player & self, const rosbag2_storage::StorageOptions & storage_options,
    PlayOptions & play_options)
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Player.play(storage_options, play_options) is "
          "deprecated. Use the parameterless play() instead.", 1);
      return self.play(storage_options, play_options);
    },
    py::arg("storage_options"),
    py::arg("play_options"),
    "Deprecated: use play() with preconfigured options instead.")

  // TODO(christophebedard): remove this method after a deprecation period
  // Deprecated play method with multiple storage options
  .def("play",
    [](rosbag2_py::Player & self,
    const std::vector<py::object> & storage_options,
    PlayOptions & play_options)
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Player.play(storage_options_list, play_options) is "
          "deprecated. Use the parameterless play() instead.", 1);
      return self.play(storage_options, play_options);
    },
    py::arg("storage_options"),
    py::arg("play_options"),
    "Deprecated: use play() with preconfigured options instead.")

  // Recommended burst playback method
  .def("burst", py::overload_cast<size_t>(&rosbag2_py::Player::burst), py::arg("num_messages"),
    R"pbdoc(
      Play a burst of messages.

      This is equivalent to the rosbag2_transport::Player::burst(const size_t) method.

      Args:
          num_messages (int): Number of messages to play in this burst.
      Returns:
          size_t: Number of messages played in this burst.
    )pbdoc")

  // TODO(christophebedard): remove this method after a deprecation period
  // Deprecated burst method with storage and play options
  .def("burst",
    [](rosbag2_py::Player & self, const rosbag2_storage::StorageOptions & storage_options,
    PlayOptions & play_options, size_t num_messages)
    {
      PyErr_WarnEx(PyExc_DeprecationWarning,
          "Player.burst(storage_options, play_options, num_messages) is deprecated. "
          "Use burst(num_messages) with preconfigured options instead.", 1);
      return self.burst(storage_options, play_options, num_messages);
    },
    py::arg("storage_options"),
    py::arg("play_options"),
    py::arg("num_messages"),
    "Deprecated: use burst(num_messages) with preconfigured options instead.")

  .def_static("cancel", &rosbag2_py::Player::cancel,
    R"pbdoc(
      Cancel the ongoing playback session.

      This is a static method and will affect any running Players globally.
      Deprecated: use Player.stop() instead.
    )pbdoc")
  ;

  py::class_<rosbag2_py::Recorder>(m, "Recorder")
  // TODO(christophebedard): remove this constructor after a deprecation period
  // Deprecated default constructor
  .def(py::init([]()
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Recorder() is deprecated. Use the constructor with "
        "full configuration parameters instead.", 1);
      return new rosbag2_py::Recorder();
    }), "Deprecated: Use constructor with full options.")

  // TODO(christophebedard): remove this constructor after a deprecation period
  // Deprecated constructor with string argument
  .def(py::init([](const std::string & arg)
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Recorder(log_level) is deprecated. Use the"
        " constructor with full configuration parameters instead.", 1);
      return new rosbag2_py::Recorder(arg);
    }), py::arg("arg"), "Deprecated: Use constructor with full options.")

  // Recommended constructor with storage and record options
  .def(
    py::init<const rosbag2_storage::StorageOptions &, RecordOptions &,
    const std::string &, const std::string &>(),
    py::arg("storage_options"),
    py::arg("record_options"),
    py::arg("log_level") = "info",
    py::arg("node_name") = "rosbag2_recorder",
    R"pbdoc(
      Initialize a Recorder with complete configuration.

      Args:
          storage_options (StorageOptions): Configuration for storage backend (e.g., URI, format).
          record_options (RecordOptions): Options for recording (e.g., topics, QoS settings).
          log_level (str, optional): Logging level, defaults to 'info'.
          node_name (str, optional): Name of the recorder node, defaults to 'rosbag2_recorder'.
    )pbdoc")

  .def("start_spin", &rosbag2_py::Recorder::start_spin,
    R"pbdoc(
      Create and start a thread with an executor that spins the recorder node.

      Waits for the executor to start spinning before returning.
    )pbdoc")

  .def("stop_spin", &rosbag2_py::Recorder::stop_spin,
    R"pbdoc(
      Stop the thread that spins the recorder node, if there is one.
    )pbdoc")

  .def("record", py::overload_cast<>(&rosbag2_py::Recorder::record),
    R"pbdoc(
      Start recording based on the internal Recorder configuration.

      This is the preferred method for starting a recording session.
      It is equivalent to the rosbag2_transport::Recorder::record() method.
      All parameters should be configured via the constructor.
    )pbdoc")

  .def(
    "stop",
    &rosbag2_py::Recorder::stop,
    "Stop recording.")

  .def(
    "pause",
    &rosbag2_py::Recorder::pause,
    "Pause the recording.")

  .def(
    "resume",
    &rosbag2_py::Recorder::resume,
    "Resume the recording.")

  .def(
    "is_paused",
    &rosbag2_py::Recorder::is_paused,
    "Whether the recording is currently paused.")

  // TODO(christophebedard): remove this method after a deprecation period
  // (deprecated) record method
  .def("record",
    [](rosbag2_py::Recorder & self, const rosbag2_storage::StorageOptions & storage_options,
    RecordOptions & record_options, const std::string & node_name)
    {
      PyErr_WarnEx(PyExc_DeprecationWarning, "Recorder.record(storage_options, record_options, "
        "node_name) is deprecated. Use the parameterless record() instead.", 1);
      return self.record(storage_options, record_options, node_name);
    },
    py::arg("storage_options"),
    py::arg("record_options"),
    py::arg("node_name") = "rosbag2_recorder",
    "Deprecated: use record() with preconfigured options instead.")

  .def_static("cancel", &rosbag2_py::Recorder::cancel,
    R"pbdoc(
      Cancel the ongoing recording session.

      This is a static method and will affect any running Recorders globally.
      Deprecated: use Recorder.stop() instead.
    )pbdoc")
  ;

  m.def(
    "bag_rewrite",
    &rosbag2_py::bag_rewrite,
    py::arg("input_options"),
    py::arg("input_config_file"),
    py::arg("output_config_file"),
    R"pbdoc(
      Rewrite one or more input bags into one or more output bags according to provided settings.

      Exactly one input source must be provided:
        - Either `input_options`: a list of StorageOptions describing input bags.
        - Or `input_config_file`: path to a YAML config file containing `input_bags` entries.

      The `output_config_file` must be a YAML config with an `output_bags` list, where each entry
      may contain StorageOptions and RecordOptions fields used to configure the output bags.

      Args:
        input_options (List[StorageOptions], optional):
          In-memory input bag configurations. Default is an empty list.
        input_config_file (str, optional):
          Path to a YAML config file that defines `input_bags`. Default is empty.
        output_config_file (str):
          Path to a YAML config file that defines `output_bags` with StorageOptions and
          RecordOptions.

      Raises:
        RuntimeError:
          - If neither or both input sources are provided.
          - If required YAML keys are missing or misformatted.
          - If any input bag URI does not exist.
    )pbdoc"
  );
}
