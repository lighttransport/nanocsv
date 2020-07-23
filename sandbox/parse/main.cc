#include <string>
#include <iostream>
#include <vector>

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

std::vector<std::string> parse_header(const char *p, const size_t len, char delimiter)
{
  std::vector<std::string> tokens;

  if (len == 0) {
    return tokens;
  }

  bool in_quoted_string = false;
  size_t s_start = 0;

  for (size_t i = 0; i < len; i++) {

    if (is_line_ending(p, i, len - 1)) {
      //std::cout << "i " << i << " is line ending\n";
      break;
    }

    if (p[i] == '"') {
      in_quoted_string = !in_quoted_string;
      continue;
    }

    if (!in_quoted_string && (p[i] == delimiter)) {
      //std::cout << "s_start = " << s_start << ", (i-1) = " << i-1 << "\n";
      //std::cout << "p[i] = " << p[i] << "\n";
      if (s_start < (i - 1)) {
        std::string tok(p + s_start, i - s_start);

        tokens.push_back(tok);
      } else {
        // Add empty string
        tokens.push_back(std::string());
      }

      s_start = i + 1;
    }
  }

  // the remainder
  //std::cout << "remain: s_start = " << s_start << ", len - 1 = " << len-1 << "\n";

  if (s_start <= (len - 1)) {
    std::string tok(p + s_start, len - s_start);
    tokens.push_back(tok);
  }

  return tokens;
}

void print_tokens(const std::vector<std::string> &tokens) {
  std::cout << "Num tokens = " << tokens.size() << "\n";

  for (size_t i = 0; i < tokens.size(); i++) {
    std::cout << "[" << i << "] = " << tokens[i] << "\n";
  }
}

int main(int argc, char **argv)
{
  {
    std::string input0 = "\" hell,o\", muda";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  {
    std::string input0 = ",";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  {
    std::string input0 = "aa,";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  {
    std::string input0 = "\"aa,";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  {
    std::string input0 = "\"bb\",\"";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  {
    std::string input0 = "\"cc\",\",";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  {
    std::string input0 = "dd,\",";
    std::vector<std::string> tokens = parse_header(input0.data(), input0.size(), ',');
    print_tokens(tokens);
  }

  return 0;
}
