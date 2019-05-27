#define NANOCSV_IMPLEMENTATION
#include "nanocsv.h"

int main(int argc, char **argv)
{
  if (argc < 2) {
    std::cout << "csv_parser_example input.csv (num_threads) (delimiter)\n";
  }

  std::string filename("./data/array-4-5.csv");
  int num_threads = -1; // -1 = use all system threads
  char delimiter = ' ';

  if (argc > 1) {
    filename = argv[1];
  }

  if (argc > 2) {
    num_threads = std::atoi(argv[2]);
  }

  if (argc > 3) {
    delimiter = argv[3][0];
  }

  nanocsv::ParseOption option;
  option.delimiter = delimiter;
  option.req_num_threads = num_threads;
  option.verbose = true;

  std::string warn;
  std::string err;

  nanocsv::CSV<float> csv;

  bool ret = nanocsv::ParseCSVFromFile(filename, option, &csv, &warn, &err);

  if (!warn.empty()) {
    std::cout << "WARN: " << warn << "\n";
  }


  if (!ret) {

    if (!err.empty()) {
      std::cout << "ERROR: " << err << "\n";
    }

    return EXIT_FAILURE;
  }

  std::cout << "num records(rows) = " << csv.num_records << "\n";
  std::cout << "num fields(columns) = " << csv.num_fields << "\n";

  // Limit printing records and fields
  const size_t print_num_records = 20;
  const size_t print_num_fields = 8;
  bool recoreds_skipped = false;

  for (size_t j = 0; j < csv.num_records; j++) {
    if ((print_num_records < j) && (j != (csv.num_records - 1))) {
      if (!recoreds_skipped) {
        std::cout << "...\n";
        recoreds_skipped = true;
      }
      continue;
    }

    std::cout << "[" << j << "] ";
    bool fields_skipped = false;
    for (size_t i = 0; i < csv.num_fields; i++) {

      if ((print_num_fields < i) && (i != (csv.num_fields - 1))) {
        if (!fields_skipped) {
          std::cout << " ... ";
          fields_skipped = true;
        }
        continue;
      }

      std::cout << csv.values[j * csv.num_fields + i];

      if (i != (csv.num_fields - 1)) {
        std::cout << ", ";
      }
    }
    std::cout << "\n";
  }

  return EXIT_SUCCESS;
}
