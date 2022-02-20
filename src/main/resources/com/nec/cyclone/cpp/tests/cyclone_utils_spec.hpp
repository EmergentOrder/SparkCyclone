/*
 * Copyright (c) 2022 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#pragma once

#include "cyclone/cyclone_utils.hpp"
#include "tests/doctest.h"

namespace cyclone::tests {
  TEST_CASE("bitmask_to_matching_ids() works") {
    std::vector<size_t> bitmask = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
    std::vector<size_t> expected = { 2, 7, 8, 9, 10, 13, 14 };
    CHECK(cyclone::bitmask_to_matching_ids(bitmask) == expected);
  }
}
