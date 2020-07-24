#include "acutest.h"

#define NANOCSV_IMPLEMENTATION
#include "nanocsv.h"

void test_nan(void) {

  std::string test_csv = "nan, -nan";

  nanocsv::ParseOption<float> option;
  option.delimiter = ',';
  option.req_num_threads = 1;
  option.verbose = true;
  option.ignore_header = true;

  std::string warn;
  std::string err;

  nanocsv::CSV<float> csv;

  // +1 for '\n'
  bool ret = nanocsv::ParseCSVFromMemory(test_csv.data(), test_csv.size() + 1, option, &csv, &warn, &err);

  if (warn.size()) {
    std::cout << warn << "\n";
  }

  if (err.size()) {
    std::cout << err << "\n";
  }

  TEST_CHECK(ret == true);
  if (!TEST_CHECK(csv.num_fields == 2)) {
    TEST_MSG("num_fields %d but expected %d", int(csv.num_fields), 2);
  }
  if (!TEST_CHECK(csv.num_records == 1)) {
    TEST_MSG("num_records %d but expected %d", int(csv.num_records), 1);
  }
  TEST_CHECK(std::isnan(csv.values[0]));
  TEST_CHECK(std::isnan(csv.values[1]));

}

void test_inf(void) {

  std::string test_csv = "inf, -inf";

  nanocsv::ParseOption<float> option;
  option.delimiter = ',';
  option.req_num_threads = 1;
  option.verbose = true;
  option.ignore_header = true;

  std::string warn;
  std::string err;

  nanocsv::CSV<float> csv;

  // +1 for '\n'
  bool ret = nanocsv::ParseCSVFromMemory(test_csv.data(), test_csv.size() + 1, option, &csv, &warn, &err);

  if (warn.size()) {
    std::cout << warn << "\n";
  }

  if (err.size()) {
    std::cout << err << "\n";
  }

  TEST_CHECK(ret == true);
  if (!TEST_CHECK(csv.num_fields == 2)) {
    TEST_MSG("num_fields %d but expected %d", int(csv.num_fields), 2);
  }
  if (!TEST_CHECK(csv.num_records == 1)) {
    TEST_MSG("num_records %d but expected %d", int(csv.num_records), 1);
  }
  TEST_CHECK(csv.values[0] == std::numeric_limits<float>::infinity());
  TEST_CHECK(csv.values[1] == -std::numeric_limits<float>::infinity());
}

void test_special(void) {

  std::string test_csv = "1.0, inf, -nan";

  nanocsv::ParseOption<float> option;
  option.delimiter = ',';
  option.req_num_threads = 1;
  option.verbose = true;
  option.ignore_header = true;

  std::string warn;
  std::string err;

  nanocsv::CSV<float> csv;

  // +1 for '\n'
  bool ret = nanocsv::ParseCSVFromMemory(test_csv.data(), test_csv.size() + 1, option, &csv, &warn, &err);

  if (warn.size()) {
    std::cout << warn << "\n";
  }

  if (err.size()) {
    std::cout << err << "\n";
  }

  TEST_CHECK(ret == true);
  if (!TEST_CHECK(csv.num_fields == 3)) {
    TEST_MSG("num_fields %d but expected %d", int(csv.num_fields), 3);
  }
  if (!TEST_CHECK(csv.num_records == 1)) {
    TEST_MSG("num_records %d but expected %d", int(csv.num_records), 1);
  }
  TEST_CHECK(std::fabs(csv.values[0] - 1.0f) < std::numeric_limits<float>::epsilon());
  TEST_CHECK(csv.values[1] == std::numeric_limits<float>::infinity());
  TEST_CHECK(std::isnan(csv.values[2]));
}

void test_na_v001(void) {

  // empty(null), nan, empty(whitespace)
  std::string test_csv = ", nan,  ";

  nanocsv::ParseOption<float> option;
  option.delimiter = ',';
  option.req_num_threads = 1;
  option.verbose = true;
  option.ignore_header = true;

  option.replace_null = true;
  option.null_value = 1.0f;

  std::string warn;
  std::string err;

  nanocsv::CSV<float> csv;

  // +1 for '\n'
  bool ret = nanocsv::ParseCSVFromMemory(test_csv.data(), test_csv.size() + 1, option, &csv, &warn, &err);

  if (warn.size()) {
    std::cout << warn << "\n";
  }

  if (err.size()) {
    std::cout << err << "\n";
  }

  TEST_CHECK(ret == true);
  if (!TEST_CHECK(csv.num_fields == 3)) {
    TEST_MSG("num_fields %d but expected %d", int(csv.num_fields), 3);
  }
  if (!TEST_CHECK(csv.num_records == 1)) {
    TEST_MSG("num_records %d but expected %d", int(csv.num_records), 1);
  }
  if (!TEST_CHECK(std::fabs(csv.values[0] - 1.0f) < std::numeric_limits<float>::epsilon())) {
    TEST_MSG("na %f but expected is %f", csv.values[0], 1.0f);
  }
  TEST_CHECK(std::isnan(csv.values[1]));
  if (!TEST_CHECK(std::fabs(csv.values[2] - 1.0f) < std::numeric_limits<float>::epsilon())) {
    TEST_MSG("na %f but expected is %f", csv.values[2], 1.0f);
  }
}

void test_na_v002(void) {

  // empty(null), nan, empty(whitespace), empty(null)
  std::string test_csv = ", nan,  ,";

  nanocsv::ParseOption<float> option;
  option.delimiter = ',';
  option.req_num_threads = 1;
  option.verbose = true;
  option.ignore_header = true;

  option.replace_na = true;
  option.na_value = 1.0f;

  std::string warn;
  std::string err;

  nanocsv::CSV<float> csv;

  // +1 for '\n'
  bool ret = nanocsv::ParseCSVFromMemory(test_csv.data(), test_csv.size() + 1, option, &csv, &warn, &err);

  if (warn.size()) {
    std::cout << warn << "\n";
  }

  if (err.size()) {
    std::cout << err << "\n";
  }

  TEST_CHECK(ret == true);
  if (!TEST_CHECK(csv.num_fields == 4)) {
    TEST_MSG("num_fields %d but expected %d", int(csv.num_fields), 3);
  }
  if (!TEST_CHECK(csv.num_records == 1)) {
    TEST_MSG("num_records %d but expected %d", int(csv.num_records), 1);
  }
  if (!TEST_CHECK(std::isnan(csv.values[0]))) {
    TEST_MSG("na %f but expected is nan", csv.values[0]);
  }
  if (!TEST_CHECK(std::fabs(csv.values[1] - 1.0f) < std::numeric_limits<float>::epsilon())) {
    TEST_MSG("null %f but expected is %f", csv.values[1], 1.0f);
  }
  if (!TEST_CHECK(std::isnan(csv.values[2]))) {
    TEST_MSG("na %f but expected is nan", csv.values[2]);
  }
  if (!TEST_CHECK(std::isnan(csv.values[3]))) {
    TEST_MSG("na %f but expected is nan", csv.values[3]);
  }
}

TEST_LIST = {
  { "test_nan", test_nan },
  { "test_inf", test_inf },
  { "test_special", test_special },
  { "test_na_v001", test_na_v001 },
  { "test_na_v002", test_na_v002 },
  { nullptr, nullptr }
};
