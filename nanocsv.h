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
    : public StackContainer<std::vector<T, StackAllocator<T, stack_capacity> >,
                            stack_capacity> {
 public:
  StackVector()
      : StackContainer<std::vector<T, StackAllocator<T, stack_capacity> >,
                       stack_capacity>() {}

  // We need to put this in STL containers sometimes, which requires a copy
  // constructor. We can't call the regular copy constructor because that will
  // take the stack buffer from the original. Here, we create an empty object
  // and make a stack buffer of its own.
  StackVector(const StackVector<T, stack_capacity> &other)
      : StackContainer<std::vector<T, StackAllocator<T, stack_capacity> >,
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

typedef StackVector<char, 256> ShortString;

#define IS_SPACE(x) (((x) == ' ') || ((x) == '\t'))
#define IS_DIGIT(x) \
  (static_cast<unsigned int>((x) - '0') < static_cast<unsigned int>(10))

#define IS_NEW_LINE(x, newline_delimiter) \
  (((x) == newline_delimiter) || ((x) == '\0'))

static inline void skip_space(const char **token) {
  while ((*token)[0] == ' ' || (*token)[0] == '\t') {
    (*token)++;
  }
}

static inline void skip_space_and_cr(const char **token) {
  while ((*token)[0] == ' ' || (*token)[0] == '\t' || (*token)[0] == '\r') {
    (*token)++;
  }
}

static inline size_t until_space(const char *token) {
  const char *p = token;
  while (p[0] != '\0' && p[0] != ' ' && p[0] != '\t' && p[0] != '\r') {
    p++;
  }

  return size_t(p - token);
}

static inline int length_until_newline(const char *token, int n) {
  int len = 0;

  // Assume token[n-1] = '\0'
  for (len = 0; len < n - 1; len++) {
    if (token[len] == '\n') {
      break;
    }
    if ((token[len] == '\r') && ((len < (n - 2)) && (token[len + 1] != '\n'))) {
      break;
    }
  }

  return len;
}

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

static inline bool parseString(ShortString *s, const char **token) {
  skip_space(token);
  size_t e = until_space((*token));
  (*s)->insert((*s)->end(), (*token), (*token) + e);
  (*token) += e;
  return true;
}

static inline int parseInt(const char **token) {
  skip_space(token);
  int i = my_atoi((*token));
  (*token) += until_space((*token));
  return i;
}

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
      // pow(10.0, -read)
      double frac_value = 1.0;
      for (int f = 0; f < read; f++) {
        frac_value *= 0.1;
      }
      mantissa += static_cast<int>(*curr - 0x30) * frac_value;
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
    exponent *= (exp_sign == '+' ? 1 : -1);
    if (read == 0) goto fail;
  }

assemble :

{
  double a = 1.0; /* = pow(5.0, exponent); */
  double b = 1.0; /* = 2.0^exponent */
  int i;
  for (i = 0; i < exponent; i++) {
    a = a * 5.0;
  }

  for (i = 0; i < exponent; i++) {
    b = b * 2.0;
  }

  if (exp_sign == '-') {
    a = 1.0 / a;
    b = 1.0 / b;
  }

  // (sign == '+' ? 1 : -1) * ldexp(mantissa * pow(5.0, exponent), exponent);
  *result = (sign == '+' ? 1 : -1) * (mantissa * a * b);
}

  return true;
fail:
  return false;
}

static inline float parseFloat(const char **token) {
  skip_space(token);
#ifdef TINY_OBJ_LOADER_OLD_FLOAT_PARSER
  float f = static_cast<float>(atof(*token));
  (*token) += strcspn((*token), " \t\r");
#else
  const char *end = (*token) + until_space((*token));
  double val = 0.0;
  tryParseDouble((*token), end, &val);
  float f = static_cast<float>(val);
  (*token) = end;
#endif
  return f;
}

static inline double parseDouble(const char **token) {
  skip_space(token);
  const char *end = (*token) + until_space((*token));
  double val = 0.0;
  tryParseDouble((*token), end, &val);
  (*token) = end;
  return val;
}

class ParseOption {
 public:
  ParseOption() : req_num_threads(-1), ignore_header(false), verbose(false) {}

  int req_num_threads;
  bool ignore_header;
  bool verbose;
};

/// Parse wavefront .obj(.obj string data is expanded to linear char array
/// `buf')
///
/// @param[in] buffer Input buffer(raw CSV text)
/// @param[in] buffer_length Byte length of input buffer.
/// @param[in] option Parse option.
/// @param[out] csv Parsed CSV data.
/// @param[out] warn Warning message. Verbose message will be set when
/// `option.verbose` is set to true.
/// @param[out] err Error message(filled when return value is false)
/// @return true upon success. false when error happens during parsing.
//
template <typename T>
bool ParseCSVFromMemory(const char *buffer, const size_t buffer_length,
                        const ParseOption &option, CSV<T> *csv,
                        std::string *warn, std::string *err);

}  // namespace nanocsv

#endif  // NANOCSV_H_

#ifdef NANOCSV_IMPLEMENTATION

namespace nanocsv {

///
/// Simple stream reader
///
class StreamReader {
  static inline void swap2(unsigned short *val) {
    unsigned short tmp = *val;
    uint8_t *dst = reinterpret_cast<uint8_t *>(val);
    uint8_t *src = reinterpret_cast<uint8_t *>(&tmp);

    dst[0] = src[1];
    dst[1] = src[0];
  }

  static inline void swap4(uint32_t *val) {
    uint32_t tmp = *val;
    uint8_t *dst = reinterpret_cast<uint8_t *>(val);
    uint8_t *src = reinterpret_cast<uint8_t *>(&tmp);

    dst[0] = src[3];
    dst[1] = src[2];
    dst[2] = src[1];
    dst[3] = src[0];
  }

  static inline void swap4(int *val) {
    int tmp = *val;
    uint8_t *dst = reinterpret_cast<uint8_t *>(val);
    uint8_t *src = reinterpret_cast<uint8_t *>(&tmp);

    dst[0] = src[3];
    dst[1] = src[2];
    dst[2] = src[1];
    dst[3] = src[0];
  }

  static inline void swap8(uint64_t *val) {
    uint64_t tmp = (*val);
    uint8_t *dst = reinterpret_cast<uint8_t *>(val);
    uint8_t *src = reinterpret_cast<uint8_t *>(&tmp);

    dst[0] = src[7];
    dst[1] = src[6];
    dst[2] = src[5];
    dst[3] = src[4];
    dst[4] = src[3];
    dst[5] = src[2];
    dst[6] = src[1];
    dst[7] = src[0];
  }

  static inline void swap8(int64_t *val) {
    int64_t tmp = (*val);
    uint8_t *dst = reinterpret_cast<uint8_t *>(val);
    uint8_t *src = reinterpret_cast<uint8_t *>(&tmp);

    dst[0] = src[7];
    dst[1] = src[6];
    dst[2] = src[5];
    dst[3] = src[4];
    dst[4] = src[3];
    dst[5] = src[2];
    dst[6] = src[1];
    dst[7] = src[0];
  }

  static void cpy4(int *dst_val, const int *src_val) {
    unsigned char *dst = reinterpret_cast<unsigned char *>(dst_val);
    const unsigned char *src = reinterpret_cast<const unsigned char *>(src_val);

    dst[0] = src[0];
    dst[1] = src[1];
    dst[2] = src[2];
    dst[3] = src[3];
  }

  static void cpy4(uint32_t *dst_val, const uint32_t *src_val) {
    unsigned char *dst = reinterpret_cast<unsigned char *>(dst_val);
    const unsigned char *src = reinterpret_cast<const unsigned char *>(src_val);

    dst[0] = src[0];
    dst[1] = src[1];
    dst[2] = src[2];
    dst[3] = src[3];
  }

 public:
  explicit StreamReader(const uint8_t *binary, const size_t length,
                        const bool swap_endian)
      : binary_(binary), length_(length), swap_endian_(swap_endian), idx_(0) {
    (void)pad_;
  }

  bool seek_set(const uint64_t offset) {
    if (offset > length_) {
      return false;
    }

    idx_ = offset;
    return true;
  }

  bool seek_from_currect(const int64_t offset) {
    if ((int64_t(idx_) + offset) < 0) {
      return false;
    }

    if (size_t((int64_t(idx_) + offset)) > length_) {
      return false;
    }

    idx_ = size_t(int64_t(idx_) + offset);
    return true;
  }

  size_t read(const size_t n, const uint64_t dst_len, uint8_t *dst) {
    size_t len = n;
    if ((idx_ + len) > length_) {
      len = length_ - idx_;
    }

    if (len > 0) {
      if (dst_len < len) {
        // dst does not have enough space. return 0 for a while.
        return 0;
      }

      memcpy(dst, &binary_[idx_], len);
      idx_ += len;
      return len;

    } else {
      return 0;
    }
  }

  bool read1(uint8_t *ret) {
    if ((idx_ + 1) > length_) {
      return false;
    }

    const uint8_t val = binary_[idx_];

    (*ret) = val;
    idx_ += 1;

    return true;
  }

  bool read_bool(bool *ret) {
    if ((idx_ + 1) > length_) {
      return false;
    }

    const char val = static_cast<const char>(binary_[idx_]);

    (*ret) = bool(val);
    idx_ += 1;

    return true;
  }

  bool read1(char *ret) {
    if ((idx_ + 1) > length_) {
      return false;
    }

    const char val = static_cast<const char>(binary_[idx_]);

    (*ret) = val;
    idx_ += 1;

    return true;
  }

#if 0
  bool read2(unsigned short *ret) {
    if ((idx_ + 2) > length_) {
      return false;
    }

    unsigned short val =
        *(reinterpret_cast<const unsigned short *>(&binary_[idx_]));

    if (swap_endian_) {
      swap2(&val);
    }

    (*ret) = val;
    idx_ += 2;

    return true;
  }
#endif

  bool read4(uint32_t *ret) {
    if ((idx_ + 4) > length_) {
      return false;
    }

    // use cpy4 considering unaligned access.
    const uint32_t *ptr = reinterpret_cast<const uint32_t *>(&binary_[idx_]);
    uint32_t val;
    cpy4(&val, ptr);

    if (swap_endian_) {
      swap4(&val);
    }

    (*ret) = val;
    idx_ += 4;

    return true;
  }

  bool read4(int *ret) {
    if ((idx_ + 4) > length_) {
      return false;
    }

    // use cpy4 considering unaligned access.
    const int32_t *ptr = reinterpret_cast<const int32_t *>(&binary_[idx_]);
    int32_t val;
    cpy4(&val, ptr);

    if (swap_endian_) {
      swap4(&val);
    }

    (*ret) = val;
    idx_ += 4;

    return true;
  }

  bool read8(uint64_t *ret) {
    if ((idx_ + 8) > length_) {
      return false;
    }

    uint64_t val = *(reinterpret_cast<const uint64_t *>(&binary_[idx_]));

    if (swap_endian_) {
      swap8(&val);
    }

    (*ret) = val;
    idx_ += 8;

    return true;
  }

  bool read8(int64_t *ret) {
    if ((idx_ + 8) > length_) {
      return false;
    }

    int64_t val = *(reinterpret_cast<const int64_t *>(&binary_[idx_]));

    if (swap_endian_) {
      swap8(&val);
    }

    (*ret) = val;
    idx_ += 8;

    return true;
  }

  bool read_float(float *ret) {
    if (!ret) {
      return false;
    }

    float value;
    if (!read4(reinterpret_cast<int *>(&value))) {
      return false;
    }

    (*ret) = value;

    return true;
  }

  bool read_double(double *ret) {
    if (!ret) {
      return false;
    }

    double value;
    if (!read8(reinterpret_cast<uint64_t *>(&value))) {
      return false;
    }

    (*ret) = value;

    return true;
  }

  bool read_string(std::string *ret) {
    if (!ret) {
      return false;
    }

    std::string value;

    // read untile '\0' or end of stream.
    for (;;) {
      char c;
      if (!read1(&c)) {
        return false;
      }

      value.push_back(c);

      if (c == '\0') {
        break;
      }
    }

    (*ret) = value;

    return true;
  }

  void skip_space() {
    char c;
    while (!read1(&c)) {
      if ((c == ' ') || (c == '\t')) {
        continue;
      }
      break;
    }
  }

  size_t tell() const { return idx_; }

  const uint8_t *data() const { return binary_; }

  bool swap_endian() const { return swap_endian_; }

  size_t size() const { return length_; }

  signed char front() {
    // TODO(LTE): Raise error when (idx_ >= length_).
    return *reinterpret_cast<const signed char *>(binary_ + idx_);
  }

  bool eof() const { return idx_ >= length_; }

 private:
  const uint8_t *binary_;
  const size_t length_;
  bool swap_endian_;
  char pad_[7];
  uint64_t idx_;  // current position index.
};

template <typename T>
bool ParseLine(const char *p, const size_t p_len, StackVector<T, 512> *values,
               bool is_header = false) {
  StreamReader reader(reinterpret_cast<const uint8_t *>(p), p_len,
                      /* endian */ false);

  // Skip leading space.
  reader.skip_space();

  if (reader.eof()) {
    // empty line
    return false;
  }

  if (!is_header) {
    if (reader.front() == '#') {  // comment line
      return false;
    }
  }

  values->clear();

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
// 4. Reconstruct final mesh data structure.

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
  std::chrono::duration<double, std::milli> ms_merge;
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
        if (new_line_found && (t < num_threads) &&
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

  auto line_sum = 0;
  for (size_t t = 0; t < size_t(num_threads); t++) {
    // std::cout << t << ": # of lines = " << line_infos[t].size() << std::endl;
    line_sum += line_infos[t].size();
  }
  // std::cout << "# of lines = " << line_sum << std::endl;

  // 2. allocate buffer
  auto t_alloc_start = std::chrono::high_resolution_clock::now();
  {
#if 0
    for (size_t t = 0; t < num_threads; t++) {
      commands[t].reserve(line_infos[t].size());
    }
#endif
  }

  ms_alloc = std::chrono::high_resolution_clock::now() - t_alloc_start;

  // 2. parse each line in parallel.
  {
    StackVector<std::thread, 16> workers;
    auto t_start = std::chrono::high_resolution_clock::now();

    for (size_t t = 0; t < size_t(num_threads); t++) {
      workers->push_back(std::thread([&, t]() {
        for (size_t i = 0; i < line_infos[t].size(); i++) {
          StackVector<float, 512> values;
          // TODO(LTE): Allow empty line before the header
          bool is_header = (t == 0) && (i == 0);
          bool ret = ParseLine(&buffer[line_infos[t][i].pos],
                               line_infos[t][i].len, &values, is_header);
          if (ret) {
            // TODO
          }
        }
      }));
    }

    for (size_t t = 0; t < workers->size(); t++) {
      workers[t].join();
    }

    auto t_end = std::chrono::high_resolution_clock::now();

    ms_parse = t_end - t_start;
  }

#if 0
  auto command_sum = 0;
  for (size_t t = 0; t < num_threads; t++) {
    // std::cout << t << ": # of commands = " << commands[t].size() <<
    // std::endl;
    command_sum += commands[t].size();
  }
  // std::cout << "# of commands = " << command_sum << std::endl;

  size_t num_v = 0;
  size_t num_vn = 0;
  size_t num_vt = 0;
  size_t num_f = 0;
  size_t num_indices = 0;
  for (size_t t = 0; t < num_threads; t++) {
    num_v += command_count[t].num_v;
    num_vn += command_count[t].num_vn;
    num_vt += command_count[t].num_vt;
    num_f += command_count[t].num_f;
    num_indices += command_count[t].num_indices;
  }

  // std::cout << "# v " << num_v << std::endl;
  // std::cout << "# vn " << num_vn << std::endl;
  // std::cout << "# vt " << num_vt << std::endl;
  // std::cout << "# f " << num_f << std::endl;

  // 4. merge
  // @todo { parallelize merge. }
  {
    auto t_start = std::chrono::high_resolution_clock::now();

    attrib->vertices.resize(num_v * 3);

    size_t v_offsets[kMaxThreads];

    v_offsets[0] = 0;

    for (size_t t = 1; t < num_threads; t++) {
      v_offsets[t] = v_offsets[t - 1] + command_count[t - 1].num_v;
    }

    StackVector<std::thread, 16> workers;

    for (size_t t = 0; t < num_threads; t++) {
      workers->push_back(std::thread([&, t]() {
        size_t v_count = v_offsets[t];

        for (size_t i = 0; i < commands[t].size(); i++) {
          if (commands[t][i].type == COMMAND_EMPTY) {
            continue;
          } else if (commands[t][i].type == COMMAND_USEMTL) {
            if (commands[t][i].material_name &&
                commands[t][i].material_name_len > 0 &&
                // check if there are still faces after this command
                face_count < num_indices) {
              // Find next face
              bool found = false;
              size_t i_start = i + 1, t_next, i_next;
              for (t_next = t; t_next < num_threads; t_next++) {
                for (i_next = i_start; i_next < commands[t_next].size();
                     i_next++) {
                  if (commands[t_next][i_next].type == COMMAND_F) {
                    found = true;
                    break;
                  }
                }
                if (found) break;
                i_start = 0;
              }
              // Assign material to this face
              if (found) {
                std::string material_name(commands[t][i].material_name,
                                          commands[t][i].material_name_len);
                for (size_t k = 0;
                     k < commands[t_next][i_next].f_num_verts.size(); k++) {
                  if (material_map.find(material_name) != material_map.end()) {
                    attrib->material_ids[face_count + k] =
                        material_map[material_name];
                  } else {
                    // Assign invalid material ID
                    // Set a different value than the default, to
                    // prevent following faces from being assigned a valid
                    // material
                    attrib->material_ids[face_count + k] = -2;
                  }
                }
              }
            }
          } else if (commands[t][i].type == COMMAND_V) {
            attrib->vertices[3 * v_count + 0] = commands[t][i].vx;
            attrib->vertices[3 * v_count + 1] = commands[t][i].vy;
            attrib->vertices[3 * v_count + 2] = commands[t][i].vz;
            v_count++;
          } else if (commands[t][i].type == COMMAND_VN) {
            attrib->normals[3 * n_count + 0] = commands[t][i].nx;
            attrib->normals[3 * n_count + 1] = commands[t][i].ny;
            attrib->normals[3 * n_count + 2] = commands[t][i].nz;
            n_count++;
          } else if (commands[t][i].type == COMMAND_VT) {
            attrib->texcoords[2 * t_count + 0] = commands[t][i].tx;
            attrib->texcoords[2 * t_count + 1] = commands[t][i].ty;
            t_count++;
          } else if (commands[t][i].type == COMMAND_F) {
            for (size_t k = 0; k < commands[t][i].f.size(); k++) {
              index_t &vi = commands[t][i].f[k];
              int vertex_index = fixIndex(vi.vertex_index, v_count);
              int texcoord_index = fixIndex(vi.texcoord_index, t_count);
              int normal_index = fixIndex(vi.normal_index, n_count);
              attrib->indices[f_count + k] =
                  index_t(vertex_index, texcoord_index, normal_index);
            }
            for (size_t k = 0; k < commands[t][i].f_num_verts.size(); k++) {
              attrib->face_num_verts[face_count + k] =
                  commands[t][i].f_num_verts[k];
            }

            f_count += commands[t][i].f.size();
            face_count += commands[t][i].f_num_verts.size();
          }
        }
      }));
    }

    for (size_t t = 0; t < workers->size(); t++) {
      workers[t].join();
    }

    // To each face with uninitialized material id,
    // assign the material id of the last face preceding it that has one
    for (size_t face_count = 1; face_count < num_indices; ++face_count)
      if (attrib->material_ids[face_count] == -1)
        attrib->material_ids[face_count] = attrib->material_ids[face_count - 1];

    auto t_end = std::chrono::high_resolution_clock::now();
    ms_merge = t_end - t_start;
  }
#endif

  auto t4 = std::chrono::high_resolution_clock::now();

#if 0
  // 5. Construct CSV information.
  {
    auto t_start = std::chrono::high_resolution_clock::now();

    // @todo { Can we boost the performance by multi-threaded execution? }
    int face_count = 0;
    shape_t shape;
    shape.face_offset = 0;
    shape.length = 0;
    int face_prev_offset = 0;
    for (size_t t = 0; t < num_threads; t++) {
      for (size_t i = 0; i < commands[t].size(); i++) {
        if (commands[t][i].type == COMMAND_O ||
            commands[t][i].type == COMMAND_G) {
          std::string name;
          if (commands[t][i].type == COMMAND_O) {
            name = std::string(commands[t][i].object_name,
                               commands[t][i].object_name_len);
          } else {
            name = std::string(commands[t][i].group_name,
                               commands[t][i].group_name_len);
          }

          if (face_count == 0) {
            // 'o' or 'g' appears before any 'f'
            shape.name = name;
            shape.face_offset = face_count;
            face_prev_offset = face_count;
          } else {
            if (shapes->size() == 0) {
              // 'o' or 'g' after some 'v' lines.
              // create a shape with null name
              shape.length = face_count - face_prev_offset;
              face_prev_offset = face_count;

              shapes->push_back(shape);

            } else {
              if ((face_count - face_prev_offset) > 0) {
                // push previous shape
                shape.length = face_count - face_prev_offset;
                shapes->push_back(shape);
                face_prev_offset = face_count;
              }
            }

            // redefine shape.
            shape.name = name;
            shape.face_offset = face_count;
            shape.length = 0;
          }
        }
        if (commands[t][i].type == COMMAND_F) {
          // Consider generation of multiple faces per `f` line by triangulation
          face_count += commands[t][i].f_num_verts.size();
        }
      }
    }

    if ((face_count - face_prev_offset) > 0) {
      shape.length = face_count - shape.face_offset;
      if (shape.length > 0) {
        shapes->push_back(shape);
      }
    } else {
      // Guess no 'v' line occurrence after 'o' or 'g', so discards current
      // shape information.
    }

    auto t_end = std::chrono::high_resolution_clock::now();

    ms_construct = t_end - t_start;
  }
#endif

  std::chrono::duration<double, std::milli> ms_total = t4 - t1;
  if (option.verbose) {
    if (warn) {
      std::stringstream ss;
      ss << "total parsing time: " << ms_total.count() << " ms\n";
      ss << "  line detection : " << ms_linedetection.count() << " ms\n";
      ss << "  alloc buf      : " << ms_alloc.count() << " ms\n";
      ss << "  parse          : " << ms_parse.count() << " ms\n";
      ss << "  merge          : " << ms_merge.count() << " ms\n";
      ss << "  construct      : " << ms_construct.count() << " ms\n";

      (*warn) += ss.str();
    }
  }

  return true;
}

}  // namespace nanocsv

#endif  // NANOCSV_IMPLEMENTATION
