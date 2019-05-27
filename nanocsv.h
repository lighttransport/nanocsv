//
// Optimized CSV parser written in C++11.
//

/*
The MIT License (MIT)

Copyright (c) 2019 Light Transport Entertainment, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
#ifndef NANOCSV_H_
#define NANOCSV_H_

#if !defined(NANOCSV_NO_IO)
#ifdef _WIN32
#define atoll(S) _atoi64(S)
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif
#endif

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <map>
#include <sstream>
#include <vector>

#if !defined(NANOCSV_NO_IO)
#include <cstdio>
#include <fstream>
#include <iostream>
#endif

#include <atomic>  // C++11
#include <chrono>  // C++11
#include <thread>  // C++11

namespace nanocsv {

// ----------------------------------------------------------------------------
// Small vector class useful for multi-threaded environment.
//
// stack_container.h
//
// Copyright (c) 2006-2008 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This allocator can be used with STL containers to provide a stack buffer
// from which to allocate memory and overflows onto the heap. This stack buffer
// would be allocated on the stack and allows us to avoid heap operations in
// some situations.
//
// STL likes to make copies of allocators, so the allocator itself can't hold
// the data. Instead, we make the creator responsible for creating a
// StackAllocator::Source which contains the data. Copying the allocator
// merely copies the pointer to this shared source, so all allocators created
// based on our allocator will share the same stack buffer.
//
// This stack buffer implementation is very simple. The first allocation that
// fits in the stack buffer will use the stack buffer. Any subsequent
// allocations will not use the stack buffer, even if there is unused room.
// This makes it appropriate for array-like containers, but the caller should
// be sure to reserve() in the container up to the stack buffer size. Otherwise
// the container will allocate a small array which will "use up" the stack
// buffer.
template <typename T, size_t stack_capacity>
class StackAllocator : public std::allocator<T> {
 public:
  typedef typename std::allocator<T>::pointer pointer;
  typedef typename std::allocator<T>::size_type size_type;

  // Backing store for the allocator. The container owner is responsible for
  // maintaining this for as long as any containers using this allocator are
  // live.
  struct Source {
    Source() : used_stack_buffer_(false) {}

    // Casts the buffer in its right type.
    T *stack_buffer() { return reinterpret_cast<T *>(stack_buffer_); }
    const T *stack_buffer() const {
      return reinterpret_cast<const T *>(stack_buffer_);
    }

    //
    // IMPORTANT: Take care to ensure that stack_buffer_ is aligned
    // since it is used to mimic an array of T.
    // Be careful while declaring any unaligned types (like bool)
    // before stack_buffer_.
    //

    // The buffer itself. It is not of type T because we don't want the
    // constructors and destructors to be automatically called. Define a POD
    // buffer of the right size instead.
    char stack_buffer_[sizeof(T[stack_capacity])];

    // Set when the stack buffer is used for an allocation. We do not track
    // how much of the buffer is used, only that somebody is using it.
    bool used_stack_buffer_;
  };

  // Used by containers when they want to refer to an allocator of type U.
  template <typename U>
  struct rebind {
    typedef StackAllocator<U, stack_capacity> other;
  };

  // For the straight up copy c-tor, we can share storage.
  StackAllocator(const StackAllocator<T, stack_capacity> &rhs)
      : source_(rhs.source_) {}

  // ISO C++ requires the following constructor to be defined,
  // and std::vector in VC++2008SP1 Release fails with an error
  // in the class _Container_base_aux_alloc_real (from <xutility>)
  // if the constructor does not exist.
  // For this constructor, we cannot share storage; there's
  // no guarantee that the Source buffer of Ts is large enough
  // for Us.
  // TODO(Google): If we were fancy pants, perhaps we could share storage
  // iff sizeof(T) == sizeof(U).
  template <typename U, size_t other_capacity>
  StackAllocator(const StackAllocator<U, other_capacity> &other)
      : source_(nullptr) {
    (void)other;
  }

  explicit StackAllocator(Source *source) : source_(source) {}

  // Actually do the allocation. Use the stack buffer if nobody has used it yet
  // and the size requested fits. Otherwise, fall through to the standard
  // allocator.
  pointer allocate(size_type n, void *hint = nullptr) {
    if (source_ != nullptr && !source_->used_stack_buffer_ &&
        n <= stack_capacity) {
      source_->used_stack_buffer_ = true;
      return source_->stack_buffer();
    } else {
      return std::allocator<T>::allocate(n, hint);
    }
  }

  // Free: when trying to free the stack buffer, just mark it as free. For
  // non-stack-buffer pointers, just fall though to the standard allocator.
  void deallocate(pointer p, size_type n) {
    if (source_ != nullptr && p == source_->stack_buffer())
      source_->used_stack_buffer_ = false;
    else
      std::allocator<T>::deallocate(p, n);
  }

 private:
  Source *source_;
};

// A wrapper around STL containers that maintains a stack-sized buffer that the
// initial capacity of the vector is based on. Growing the container beyond the
// stack capacity will transparently overflow onto the heap. The container must
// support reserve().
//
// WATCH OUT: the ContainerType MUST use the proper StackAllocator for this
// type. This object is really intended to be used only internally. You'll want
// to use the wrappers below for different types.
template <typename TContainerType, int stack_capacity>
class StackContainer {
 public:
  typedef TContainerType ContainerType;
  typedef typename ContainerType::value_type ContainedType;
  typedef StackAllocator<ContainedType, stack_capacity> Allocator;

  // Allocator must be constructed before the container!
  StackContainer() : allocator_(&stack_data_), container_(allocator_) {
    // Make the container use the stack allocation by reserving our buffer size
    // before doing anything else.
    container_.reserve(stack_capacity);
  }

  // Getters for the actual container.
  //
  // Danger: any copies of this made using the copy constructor must have
  // shorter lifetimes than the source. The copy will share the same allocator
  // and therefore the same stack buffer as the original. Use std::copy to
  // copy into a "real" container for longer-lived objects.
  ContainerType &container() { return container_; }
  const ContainerType &container() const { return container_; }

  // Support operator-> to get to the container. This allows nicer syntax like:
  //   StackContainer<...> foo;
  //   std::sort(foo->begin(), foo->end());
  ContainerType *operator->() { return &container_; }
  const ContainerType *operator->() const { return &container_; }

#ifdef UNIT_TEST
  // Retrieves the stack source so that that unit tests can verify that the
  // buffer is being used properly.
  const typename Allocator::Source &stack_data() const { return stack_data_; }
#endif

 protected:
  typename Allocator::Source stack_data_;
  unsigned char pad_[7];
  Allocator allocator_;
  ContainerType container_;

  // DISALLOW_EVIL_CONSTRUCTORS(StackContainer);
  StackContainer(const StackContainer &);
  void operator=(const StackContainer &);
};

// StackVector
//
// Example:
//   StackVector<int, 16> foo;
//   foo->push_back(22);  // we have overloaded operator->
//   foo[0] = 10;         // as well as operator[]
template <typename T, size_t stack_capacity>
class StackVector
    : public StackContainer<std::vector<T, StackAllocator<T, stack_capacity>>,
                            stack_capacity> {
 public:
  StackVector()
      : StackContainer<std::vector<T, StackAllocator<T, stack_capacity>>,
                       stack_capacity>() {}

  // We need to put this in STL containers sometimes, which requires a copy
  // constructor. We can't call the regular copy constructor because that will
  // take the stack buffer from the original. Here, we create an empty object
  // and make a stack buffer of its own.
  StackVector(const StackVector<T, stack_capacity> &other)
      : StackContainer<std::vector<T, StackAllocator<T, stack_capacity>>,
                       stack_capacity>() {
    this->container().assign(other->begin(), other->end());
  }

  StackVector<T, stack_capacity> &operator=(
      const StackVector<T, stack_capacity> &other) {
    this->container().assign(other->begin(), other->end());
    return *this;
  }

  // Vectors are commonly indexed, which isn't very convenient even with
  // operator-> (using "->at()" does exception stuff we don't want).
  T &operator[](size_t i) { return this->container().operator[](i); }
  const T &operator[](size_t i) const {
    return this->container().operator[](i);
  }
};

// ----------------------------------------------------------------------------

template <typename T>
class CSV {
 public:
  std::vector<T>
      values;  // 1D linearized array. The length is num_records * num_fields`

  std::vector<std::string> header;  // size() == num_fields

  size_t num_fields;
  size_t num_records;
};

#define IS_DIGIT(x) \
  (static_cast<unsigned int>((x) - '0') < static_cast<unsigned int>(10))

#if 0
// http://stackoverflow.com/questions/5710091/how-does-atoi-function-in-c-work
static inline int my_atoi(const char *c) {
  int value = 0;
  int sign = 1;
  if (*c == '+' || *c == '-') {
    if (*c == '-') sign = -1;
    c++;
  }
  while (((*c) >= '0') && ((*c) <= '9')) {  // isdigit(*c)
    value *= 10;
    value += int(*c - '0');
    c++;
  }
  return value * sign;
}
#endif

// Tries to parse a floating point number located at s.
//
// s_end should be a location in the string where reading should absolutely
// stop. For example at the end of the string, to prevent buffer overflows.
//
// Parses the following EBNF grammar:
//   sign    = "+" | "-" ;
//   END     = ? anything not in digit ?
//   digit   = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" ;
//   integer = [sign] , digit , {digit} ;
//   decimal = integer , ["." , integer] ;
//   float   = ( decimal , END ) | ( decimal , ("E" | "e") , integer , END ) ;
//
//  Valid strings are for example:
//   -0  +3.1417e+2  -0.0E-3  1.0324  -1.41   11e2
//
// If the parsing is a success, result is set to the parsed value and true
// is returned.
//
// The function is greedy and will parse until any of the following happens:
//  - a non-conforming character is encountered.
//  - s_end is reached.
//
// The following situations triggers a failure:
//  - s >= s_end.
//  - parse failure.
//
static bool tryParseDouble(const char *s, const char *s_end, double *result) {
  if (s >= s_end) {
    return false;
  }

  double mantissa = 0.0;
  // This exponent is base 2 rather than 10.
  // However the exponent we parse is supposed to be one of ten,
  // thus we must take care to convert the exponent/and or the
  // mantissa to a * 2^E, where a is the mantissa and E is the
  // exponent.
  // To get the final double we will use ldexp, it requires the
  // exponent to be in base 2.
  int exponent = 0;

  uint32_t abs_exponent = 0;  // = abs(exponent)

  // NOTE: THESE MUST BE DECLARED HERE SINCE WE ARE NOT ALLOWED
  // TO JUMP OVER DEFINITIONS.
  char sign = '+';
  char exp_sign = '+';
  char const *curr = s;

  // How many characters were read in a loop.
  int read = 0;
  // Tells whether a loop terminated due to reaching s_end.
  bool end_not_reached = false;

  // For parsing `.323`, `-.234f`
  bool leading_decimal_dots = false;

  /*
          BEGIN PARSING.
  */

  // Find out what sign we've got.
  if (*curr == '+' || *curr == '-') {
    sign = *curr;
    curr++;
    if ((curr != s_end) && (*curr == '.')) {
      // accept. Somethig like `.7e+2`, `-.5234`
      leading_decimal_dots = true;
    }
  } else if (IS_DIGIT(*curr)) { /* Pass through. */
  } else if (*curr == '.') {
    // accept. Somethig like `.7e+2`, `-.5234`
    leading_decimal_dots = true;
  } else {
    goto fail;
  }

  // Read the integer part.
  end_not_reached = (curr != s_end);
  if (!leading_decimal_dots) {
    while (end_not_reached && IS_DIGIT(*curr)) {
      mantissa *= 10;
      mantissa += static_cast<int>(*curr - 0x30);
      curr++;
      read++;
      end_not_reached = (curr != s_end);
    }
  }

  // We must make sure we actually got something.
  if (!leading_decimal_dots) {
    if (read == 0) goto fail;
  }

  // We allow numbers of form "#", "###" etc.
  if (!end_not_reached) goto assemble;

  // Read the decimal part.
  if (*curr == '.') {
    curr++;
    read = 1;
    end_not_reached = (curr != s_end);
    while (end_not_reached && IS_DIGIT(*curr)) {
      static const double pow_lut[] = {
          1.0,     1.0e-1,  1.0e-2,  1.0e-3,  1.0e-4,  1.0e-5,  1.0e-6,
          1.0e-7,  1.0e-8,  1.0e-9,  1.0e-10, 1.0e-11, 1.0e-12, 1.0e-13,
          1.0e-14, 1.0e-15, 1.0e-16, 1.0e-17, 1.0e-18, 1.0e-19, 1.0e-20,
          1.0e-21, 1.0e-22, 1.0e-23, 1.0e-24, 1.0e-25, 1.0e-26, 1.0e-27,
          1.0e-28, 1.0e-29, 1.0e-30, 1.0e-31,
      };
      const int lut_entries = sizeof pow_lut / sizeof pow_lut[0];

      // NOTE: Don't use powf here, it will absolutely murder precision.
      mantissa += static_cast<int>(*curr - 0x30) *
                  (read < lut_entries ? pow_lut[read] : std::pow(10.0, -read));

      read++;
      curr++;
      end_not_reached = (curr != s_end);
    }
  } else if (*curr == 'e' || *curr == 'E') {
  } else {
    goto assemble;
  }

  if (!end_not_reached) goto assemble;

  // Read the exponent part.
  if (*curr == 'e' || *curr == 'E') {
    curr++;
    // Figure out if a sign is present and if it is.
    end_not_reached = (curr != s_end);
    if (end_not_reached && (*curr == '+' || *curr == '-')) {
      exp_sign = *curr;
      curr++;
    } else if (IS_DIGIT(*curr)) { /* Pass through. */
    } else {
      // Empty E is not allowed.
      goto fail;
    }

    read = 0;
    end_not_reached = (curr != s_end);
    while (end_not_reached && IS_DIGIT(*curr)) {
      exponent *= 10;
      exponent += static_cast<int>(*curr - 0x30);
      curr++;
      read++;
      end_not_reached = (curr != s_end);
    }

    abs_exponent = static_cast<uint32_t>(exponent);
    exponent *= (exp_sign == '+' ? 1 : -1);
    if (read == 0) goto fail;
  }

assemble :

{
  double a = 1.0; /* = pow(5.0, exponent); */
  double b = 1.0; /* = 2.0^exponent */
  uint32_t i;

  for (i = 0; i < abs_exponent; i++) {
    a = a * 5.0;
  }

  for (i = 0; i < abs_exponent; i++) {
    b = b * 2.0;
  }

  if (exp_sign == '-') {
    a = 1.0 / a;
    b = 1.0 / b;
  }

  // (sign == '+' ? 1 : -1) * ldexp(mantissa * pow(5.0, exponent), exponent);
  *result = (sign == '+' ? 1 : -1) * (exponent ? (mantissa * a * b) : mantissa);
}

  return true;
fail:
  return false;
}

class ParseOption {
 public:
  ParseOption()
      : req_num_threads(-1),
        ignore_header(false),
        verbose(false),
        delimiter(' ') {}

  int req_num_threads;
  bool ignore_header;
  bool verbose;
  char delimiter;
};

///
/// Parse CSV data from memory(byte array).
///
/// @param[in] buffer Input buffer(raw CSV text)
/// @param[in] buffer_length Byte length of input buffer.
/// @param[in] option Parse option.
/// @param[out] csv Parsed CSV data.
/// @param[out] warn Warning message. Verbose message will be set when
/// `option.verbose` is set to true.
/// @param[out] err Error message(filled when return value is false)
/// @return true upon success. false when error happens during parsing.
///
template <typename T>
bool ParseCSVFromMemory(const char *buffer, const size_t buffer_length,
                        const ParseOption &option, CSV<T> *csv,
                        std::string *warn, std::string *err);

#if !defined(NANOCSV_NO_IO)

///
/// Parse CSV data from a file.
///
/// @param[in] filename CSV filename.
/// @param[in] option Parse option.
/// @param[out] csv Parsed CSV data.
/// @param[out] warn Warning message. Verbose message will be set when
/// `option.verbose` is set to true.
/// @param[out] err Error message(filled when return value is false)
/// @return true upon success. false when error happens during parsing.
///
template <typename T>
bool ParseCSVFromFile(const std::string &filename, const ParseOption &option,
                      CSV<T> *csv, std::string *warn, std::string *err);
#endif

}  // namespace nanocsv

#endif  // NANOCSV_H_

#ifdef NANOCSV_IMPLEMENTATION

namespace nanocsv {

//
// Parse single line and store results to `values`.
// Assume input line is not a header line
//
template <typename T>
bool ParseLine(const char *p, const size_t p_len, StackVector<T, 512> *values,
               const char delimiter) {
  if ((p == nullptr) || (p_len < 1)) {
    return false;
  }

  size_t loc = 0;
  if (delimiter != ' ') {
    // skip leading space.
    for (size_t i = loc; i < p_len; i++, loc++) {
      bool isspace = (p[i] == ' ') || (p[i] == '\t');
      if (!isspace) {
        break;
      }
    }
  }

  if (p[0] == '#') {  // comment line
    return false;
  }

  (*values)->clear();

  while (loc < p_len) {
    if (p[loc] == '\0') {
      // ???
      return false;
    }

    // find delimiter
    size_t delimiter_loc = loc;
    for (size_t i = loc; i < p_len; i++, delimiter_loc++) {
      if ((p[i] == delimiter) || (p[i] == '\0')) {
        break;
      }
    }

    if (loc == delimiter_loc) {
      // something like ",,2.3,,,"
      loc++;
      continue;
    }

    double value;
    if (tryParseDouble(p + loc, p + delimiter_loc, &value)) {
      (*values)->push_back(static_cast<T>(value));
    } else {
      // TODO(LTE): Report error.
    }

    // move to the next of delimiter character.
    loc = delimiter_loc + 1;

    if (delimiter != ' ') {
      // skip leading space.
      for (size_t i = loc; i < p_len; i++, loc++) {
        bool isspace = (p[i] == ' ') || (p[i] == '\t');
        if (!isspace) {
          break;
        }
      }
    }
  }

  if ((*values)->size() > 0) {
    return true;
  }

  return false;
}

typedef struct {
  size_t pos;
  size_t len;
} LineInfo;

// Idea come from https://github.com/antonmks/nvParse
// 1. mmap file
// 2. find newline(\n, \r\n, \r) and list of line data.
// 3. Do parallel parsing for each line.
// 4. Reconstruct final CSV data structure.

// Raise # of max threads if you have more CPU cores...
// In 2019, 64 cores(128 threads) are upper bounds in high-end workstaion PC.
constexpr int kMaxThreads = 128;

static inline bool is_line_ending(const char *p, size_t i, size_t end_i) {
  if (p[i] == '\0') return true;
  if (p[i] == '\n') return true;  // this includes \r\n
  if (p[i] == '\r') {
    if (((i + 1) < end_i) && (p[i + 1] != '\n')) {  // detect only \r case
      return true;
    }
  }
  return false;
}

template <typename T>
bool ParseCSVFromMemory(const char *buffer, const size_t buffer_length,
                        const ParseOption &option, CSV<T> *csv,
                        std::string *warn, std::string *err) {
  if (buffer_length < 1) {
    if (err) {
      (*err) = "`buffer_length` too short.\n";
    }
    return false;
  }

  csv->values.clear();
  csv->num_records = 0;
  csv->num_fields = 0;

  auto num_threads = (option.req_num_threads < 0)
                         ? int(std::thread::hardware_concurrency())
                         : option.req_num_threads;
  num_threads =
      (std::max)(1, (std::min)(static_cast<int>(num_threads), kMaxThreads));

  if (option.verbose) {
    if (warn) {
      std::stringstream ss;
      ss << "# of threads = " << std::to_string(num_threads) << "\n";
      (*warn) += ss.str();
    }
  }

  auto t1 = std::chrono::high_resolution_clock::now();

  StackVector<std::vector<LineInfo>, kMaxThreads> line_infos;
  line_infos->resize(kMaxThreads);

  for (size_t t = 0; t < static_cast<size_t>(num_threads); t++) {
    // Pre allocate enough memory. len / 128 / num_threads is just a heuristic
    // value.
    line_infos[t].reserve(buffer_length / 128 / size_t(num_threads));
  }

  std::chrono::duration<double, std::milli> ms_linedetection;
  std::chrono::duration<double, std::milli> ms_alloc;
  std::chrono::duration<double, std::milli> ms_parse;
  std::chrono::duration<double, std::milli> ms_construct;

  // 1. Find '\n' and create line data.
  {
    StackVector<std::thread, kMaxThreads> workers;

    auto start_time = std::chrono::high_resolution_clock::now();
    auto chunk_size = buffer_length / size_t(num_threads);

    for (size_t t = 0; t < static_cast<size_t>(num_threads); t++) {
      workers->push_back(std::thread([&, t]() {
        auto start_idx = (t + 0) * chunk_size;
        auto end_idx = (std::min)((t + 1) * chunk_size, buffer_length - 1);
        if (t == static_cast<size_t>((num_threads - 1))) {
          end_idx = buffer_length - 1;
        }

        // true if the line currently read must be added to the current line
        // info
        bool new_line_found =
            (t == 0) || is_line_ending(buffer, start_idx - 1, end_idx);

        size_t prev_pos = start_idx;
        for (size_t i = start_idx; i < end_idx; i++) {
          if (is_line_ending(buffer, i, end_idx)) {
            if (!new_line_found) {
              // first linebreak found in (chunk > 0), and a line before this
              // linebreak belongs to previous chunk, so skip it.
              prev_pos = i + 1;
              new_line_found = true;
            } else {
              LineInfo info;
              info.pos = prev_pos;
              info.len = i - prev_pos;

              if (info.len > 0) {
                line_infos[t].push_back(info);
              }

              prev_pos = i + 1;
            }
          }
        }

        // If at least one line started in this chunk, find where it ends in the
        // rest of the buffer
        if (new_line_found && (t < size_t(num_threads)) &&
            (buffer[end_idx - 1] != '\n')) {
          for (size_t i = end_idx; i < buffer_length; i++) {
            if (is_line_ending(buffer, i, buffer_length)) {
              LineInfo info;
              info.pos = prev_pos;
              info.len = i - prev_pos;

              if (info.len > 0) {
                line_infos[t].push_back(info);
              }

              break;
            }
          }
        }
      }));
    }

    for (size_t t = 0; t < workers->size(); t++) {
      workers[t].join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();

    ms_linedetection = end_time - start_time;
  }

  auto num_records = 0;
  for (size_t t = 0; t < size_t(num_threads); t++) {
    num_records += line_infos[t].size();
  }

  if (option.verbose && warn) {
    (*warn) += "# of records = " + std::to_string(num_records) + "\n";
  }

  // 2. allocate per thread buffer
  auto t_alloc_start = std::chrono::high_resolution_clock::now();

  std::vector<std::vector<float>> line_buffer;
  line_buffer.resize(size_t(num_threads));
  {
    for (size_t t = 0; t < size_t(num_threads); t++) {
      // Heuristics. byte size = # of chars / 4
      line_buffer[t].reserve(line_infos[t].size() / 4);
    }
  }

  ms_alloc = std::chrono::high_resolution_clock::now() - t_alloc_start;

  // Records # of fields per thread.
  // After finishing parsing, check if these values are all same.
  std::vector<int> num_fields_per_thread(size_t(num_threads), -1);

  // 3. parse each line in parallel.
  {
    StackVector<std::thread, kMaxThreads> workers;
    auto t_start = std::chrono::high_resolution_clock::now();

    bool invalid_csv = false;

    for (size_t t = 0; t < size_t(num_threads); t++) {
      workers->push_back(std::thread([&, t]() {
        for (size_t i = 0; i < line_infos[t].size(); i++) {
          StackVector<float, 512> values;
          // TODO(LTE): Allow empty line before the header
          // bool is_header = (t == 0) && (i == 0);
          // TODO(LTE): parse header string
          bool ret = ParseLine(&buffer[line_infos[t][i].pos],
                               line_infos[t][i].len, &values, option.delimiter);
          if (ret) {
            if (num_fields_per_thread[t] == -1) {
              num_fields_per_thread[t] = int(values->size());
            }

            // All records should have same number of fields.
            if (num_fields_per_thread[t] != int(values->size())) {
              invalid_csv = true;
              break;
            }

            for (size_t k = 0; k < values->size(); k++) {
              line_buffer[t].push_back(values[k]);
            }
          }
        }
      }));
    }

    for (size_t t = 0; t < workers->size(); t++) {
      workers[t].join();
    }

    auto t_end = std::chrono::high_resolution_clock::now();

    ms_parse = t_end - t_start;

    if (invalid_csv) {
      if (err) {
        // TODO(LTE): Show the linue number of invalid record.
        (*err) += "The number of fields must be same for all records.\n";
      }
      return false;
    }
  }

  // Check if all records have same the number of fields.

  int num_fields = num_fields_per_thread[0];
  if (num_fields <= 0) {
    if (err) {
      (*err) += "It looks thread 0 failed to parse CSV records.\n";
    }
    return false;
  }

  for (size_t i = 1; i < num_fields_per_thread.size(); i++) {
    if (num_fields_per_thread[i] != -1) {
      if (num_fields_per_thread[i] != num_fields) {
        if (err) {
          std::stringstream ss;
          ss << "Thread " << i << " has num_fields " << num_fields_per_thread[i]
             << " but it should be " << num_fields << " (thread 0)\n";
          (*err) += ss.str();
        }
        return false;
      }
    }
  }

  // 4. Construct CSV information.
  {
    auto t_start = std::chrono::high_resolution_clock::now();

    csv->num_fields = size_t(num_fields);

    // Offset index to output csv.values.
    StackVector<size_t, kMaxThreads> offset_table;
    offset_table->resize(size_t(num_threads));

    // Compute offset index and count actual numer of records.
    size_t num_actual_records = 0;
    size_t offset = 0;
    for (size_t t = 0; t < size_t(num_threads); t++) {
      offset_table[t] = offset;

      // Assume all record has same number of fields,
      // so dividing the number of fields gives the number of records.j
      offset += line_buffer[t].size();  // = num_fields * num_records per thread
      num_actual_records += line_buffer[t].size() / size_t(num_fields);
    }

    csv->num_records = num_actual_records;

    csv->values.resize(csv->num_fields * csv->num_records);

    StackVector<std::thread, kMaxThreads> workers;

    for (size_t t = 0; t < size_t(num_threads); t++) {
      workers->push_back(std::thread([&, t]() {
        memcpy(csv->values.data() + offset_table[t], line_buffer[t].data(),
               sizeof(T) * line_buffer[t].size());
      }));
    }

    for (size_t t = 0; t < workers->size(); t++) {
      workers[t].join();
    }

    auto t_end = std::chrono::high_resolution_clock::now();

    ms_construct = t_end - t_start;
  }

  auto t4 = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double, std::milli> ms_total = t4 - t1;
  if (option.verbose) {
    if (warn) {
      std::stringstream ss;
      ss << "total parsing time: " << ms_total.count() << " ms\n";
      ss << "  line detection : " << ms_linedetection.count() << " ms\n";
      ss << "  alloc buf      : " << ms_alloc.count() << " ms\n";
      ss << "  parse          : " << ms_parse.count() << " ms\n";
      ss << "  construct      : " << ms_construct.count() << " ms\n";

      (*warn) += ss.str();
    }
  }

  return true;
}

#if !defined(NANOCSV_NO_IO)
template <typename T>
bool ParseCSVFromFile(const std::string &filename, const ParseOption &option,
                      CSV<T> *csv, std::string *warn, std::string *err) {
  std::ifstream ifs(filename);
  if (!ifs) {
    if (err) {
      (*err) =
          "Failed to open file. File may not be found : " + filename + "\n";
    }
    return false;
  }

  ifs.seekg(0, ifs.end);
  size_t sz = static_cast<size_t>(ifs.tellg());
  ifs.seekg(0, ifs.beg);

  if (sz < 1) {
    if (err) {
      (*err) = "File size is zero : " + filename + "\n";
    }
    return false;
  }

  std::vector<char> buffer;
  buffer.resize(sz);

  ifs.read(buffer.data(), static_cast<std::streamsize>(sz));

  return ParseCSVFromMemory(buffer.data(), sz, option, csv, warn, err);
}
#endif

}  // namespace nanocsv

#endif  // NANOCSV_IMPLEMENTATION
