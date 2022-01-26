#include "transfer-definitions.hpp"
#include "cyclone.hpp"
#include <cmath>
#include <bitset>
#include <string>
#include <vector>
#include <iostream>
#include <tuple>
#include "tuple_hash.hpp"
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/dataframe/join.hpp"
#include "frovedis/core/set_operations.hpp"

nullable_varchar_vector * from_vec(const std::vector<std::string> &data) {
  auto *vec = new nullable_varchar_vector;
  vec->count = data.size();

  {
    vec->dataSize = 0;
    for (auto i = 0; i < data.size(); i++) {
      vec->dataSize += data[i].size();
    }
  }

  {
    vec->data = new char[vec->dataSize];
    auto p = 0;
    for (auto i = 0; i < data.size(); i++) {
      for (auto j = 0; j < data[i].size(); j++) {
        vec->data[p++] = data[i][j];
      }
    }
  }

  {
    vec->offsets = new int32_t[data.size() + 1];
    vec->offsets[0] = 0;
    for (auto i = 0; i < data.size(); i++) {
      vec->offsets[i+1] = vec->offsets[i] + data[i].size();
    }
  }

  {
    size_t vcount = ceil(data.size() / 64.0);
    vec->validityBuffer = new uint64_t[vcount];
    for (auto i = 0; i < vcount; i++) {
      vec->validityBuffer[i] = 0xffffffffffffffff;
    }
  }

  return vec;
}

nullable_varchar_vector * filter_vec(const nullable_varchar_vector *input,
                                      const std::vector<size_t> &matching_ids) {
  auto *output = new nullable_varchar_vector;

  frovedis::words output_input_words = varchar_vector_to_words(input);
  std::vector<size_t> output_starts(matching_ids.size());
  std::vector<size_t> output_lens(matching_ids.size());
  for ( int g = 0; g < matching_ids.size(); g++ ) {
    int i = matching_ids[g];
    output_starts[g] = output_input_words.starts[i];
    output_lens[g] = output_input_words.lens[i];
  }
  std::vector<size_t> output_new_starts;
  std::vector<int> output_new_chars = frovedis::concat_words(
    output_input_words.chars,
    (const std::vector<size_t>&)(output_starts),
    (const std::vector<size_t>&)(output_lens),
    "",
    (std::vector<size_t>&)(output_new_starts)
  );
  output_input_words.chars = output_new_chars;
  output_input_words.starts = output_new_starts;
  output_input_words.lens = output_lens;
  words_to_varchar_vector(output_input_words, output);

  for ( int g = 0; g < matching_ids.size(); g++ ) {
    int i = matching_ids[g];
    set_validity(output->validityBuffer, g, check_valid(input->validityBuffer, i));
  }

  return output;
}

nullable_varchar_vector * project_eval(const nullable_varchar_vector *input_0)  {
  auto *output_0 = new nullable_varchar_vector;

  frovedis::words output_0_input_words = varchar_vector_to_words(input_0);
  std::vector<size_t> output_0_starts(input_0->count);
  std::vector<size_t> output_0_lens(input_0->count);
  for ( int32_t i = 0; i < input_0->count; i++ ) {
    output_0_starts[i] = output_0_input_words.starts[i];
    output_0_lens[i] = output_0_input_words.lens[i];
  }
  std::vector<size_t> output_0_new_starts;
  std::vector<int> output_0_new_chars = frovedis::concat_words(
    output_0_input_words.chars,
    (const std::vector<size_t>&)(output_0_starts),
    (const std::vector<size_t>&)(output_0_lens),
    "",
    (std::vector<size_t>&)(output_0_new_starts)
  );
  output_0_input_words.chars = output_0_new_chars;
  output_0_input_words.starts = output_0_new_starts;
  output_0_input_words.lens = output_0_lens;
  words_to_varchar_vector(output_0_input_words, output_0);

  for ( int i = 0; i < output_0->count; i++ ) {
    set_validity(output_0->validityBuffer, i, check_valid(input_0->validityBuffer, i));
  }

  return output_0;
}

void filter_test() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };
  std::vector<size_t> matching_ids { 1, 3, 5 };

  const auto *input = from_vec(data);
  set_validity(input->validityBuffer, 3, 0);

  std::cout << "================================================================================" << std::endl;
  std::cout << "FILTER TEST\n" << std::endl;

  std::cout << "Original nullable_varchar_vector:" << std::endl;
  debug_nullable_varchar_vector(input);

  const auto *output = filter_vec(input, matching_ids);
  std::cout << "Filtered nullable_varchar_vector:" << std::endl;
  debug_nullable_varchar_vector(output);

  std::cout << "================================================================================" << std::endl;
  return;
}

void projection_test() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };

  const auto *input = from_vec(data);
  set_validity(input->validityBuffer, 3, 0);

  std::cout << "================================================================================" << std::endl;
  std::cout << "PROJECTION TEST\n" << std::endl;

  std::cout << "Original nullable_varchar_vector:" << std::endl;
  debug_nullable_varchar_vector(input);

  auto *output = project_eval(input);
  std::cout << "Cloned nullable_varchar_vector:" << std::endl;
  debug_nullable_varchar_vector(output);

  std::cout << "================================================================================" << std::endl;
  return;
}

int main() {
  projection_test();
  filter_test();
}
