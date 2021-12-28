/*
 * Copyright (c) 2021 Xpress AI.
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <limits>
#include <iostream>
#include <vector>
#include <chrono>
#include <ctime>
#include <algorithm>
#include "words.hpp"
#include "words.cc"
#include "char_int_conv.hpp"
#include "char_int_conv.cc"
#include "parsefloat.hpp"
#include "parsefloat.cc"


#ifndef VE_TD_DEFS
typedef struct
{
    void **data;
    size_t count;
    size_t size;
} data_out;

typedef struct
{
    char *data;
    int32_t *offsets;
    int32_t count;
} varchar_vector;

typedef struct
{
    int32_t *data;
    uint64_t *validityBuffer;
    int32_t count;
} nullable_int_vector;

typedef struct
{
    double *data;
    uint64_t *validityBuffer;
    int32_t count;
} nullable_double_vector;

typedef struct
{
    int64_t *data;
    uint64_t *validityBuffer;
    int32_t count;
} nullable_bigint_vector;


typedef struct
{
    int32_t *data;
    int32_t *offsets;
    int32_t dataSize;
    int32_t count;
    int32_t *lengths;
} non_null_varchar_vector;

typedef struct
{
    int32_t *data;
    int32_t *offsets;
    uint64_t *validityBuffer;
    int32_t dataSize;
    int32_t count;
    int32_t *lengths;
} nullable_varchar_vector;

static std::string utcnanotime() {
    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::system_clock::to_time_t(now);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count() % 1000000000;
    char utc[32];
    strftime(utc, 32, "%FT%T", gmtime(&seconds));
    snprintf(strchr(utc, 0), 32 - strlen(utc), ".%09ldZ", ns);
    return utc;
}

inline void log(std::string msg) {
    std::cout << utcnanotime().c_str() << " " << msg.c_str() << std::endl;
}

inline void set_validity(uint64_t *validityBuffer, int32_t idx, int32_t validity) {
    int32_t byte = idx / 64;
    int32_t bitIndex = idx % 64;

    if (validity) {
        validityBuffer[byte] |= (1UL << bitIndex);
    } else {
        validityBuffer[byte] &= ~(1UL << bitIndex);
    }
}

inline uint64_t check_valid(uint64_t *validityBuffer, int32_t idx) {
    uint64_t byte = idx / 64;
    uint64_t bitIndex = idx % 64;
    uint64_t res = (validityBuffer[byte] >> bitIndex) & 1;

    return res;
}

frovedis::words data_offsets_to_words(
    const int32_t *data,
    const int32_t *offsets,
    const int32_t *lengths,
    /** size of all the data **/
    const int32_t size,
    /** count of the words **/
    const int32_t count
    ) {
    #ifdef DEBUG
        std::cout << "data: '";
        for (int i = 0; i < size; i++) {
            std::cout << (char)data[i];
        }
        std::cout << "'" << std::endl;

        std::cout << "offsets: ";
        for (int i = 0; i < count; i++) {
            std::cout << offsets[i] << ", ";
        }

        std::cout << "lengths: ";
        for (int i = 0; i < count; i++) {
            std::cout << lengths[i] << ", ";
        }
        std::cout << std::endl;
    #endif

    frovedis::words ret;
    if (count == 0) {
        return ret;
    }

    #ifdef DEBUG
        std::cout << "count: " << count << std::endl;
    #endif

    ret.lens.resize(count);
    for (int i = 0; i < count; i++) {
        ret.lens[i] = lengths[i];
    }
    ret.starts.resize(count);
    for (int i = 0; i < count; i++) {
        ret.starts[i] = offsets[i];
    }

    ret.chars.assign(data, data + size);

    return ret;
}

frovedis::words varchar_vector_to_words(const non_null_varchar_vector *v) {
    return data_offsets_to_words(v->data, v->offsets, v->lengths, v->dataSize, v->count);
}

frovedis::words varchar_vector_to_words(const nullable_varchar_vector *v) {
    return data_offsets_to_words(v->data, v->offsets, v->lengths, v->dataSize, v->count);
}

void words_to_varchar_vector(frovedis::words& in, nullable_varchar_vector *out) {
    #ifdef DEBUG
        std::cout << utcnanotime().c_str() << " $$ " << "words_to_varchar_vector" << std::endl << std::flush;
    #endif

    out->count = in.lens.size();

    #ifdef DEBUG
    std::cout << "out->count = " << out->count << std::endl;
    #endif

    out->dataSize = in.chars.size();

    #ifdef DEBUG
    std::cout << "out->dataSize = " << out->dataSize << std::endl;
    #endif

    out->data = (int32_t *)malloc(in.chars.size() * sizeof(int32_t));
    if (out->data == NULL) {
        std::cout << "Failed to malloc " << out->dataSize << " * sizeof(int32_t)." << std::endl;
        return;
    }
    std::copy(in.chars.begin(), in.chars.end(), out->data);

    out->offsets = (int32_t *)malloc((in.starts.size()) * sizeof(int32_t));
    for (int i = 0; i < in.starts.size(); i++) {
        out->offsets[i] = in.starts[i];
    }

    out->lengths = (int32_t *)malloc(in.lens.size() * sizeof(int32_t));
    for (int i = 0; i < in.lens.size(); i++) {
        out->lengths[i] = in.lens[i];
    }

    #ifdef DEBUG
        std::cout << "data: '";
        for (int i = 0; i < out->dataSize; i++) {
            std::cout << (char)out->data[i];
        }
        std::cout << "'" << std::endl;

        std::cout << "offsets: ";
        for (int i = 0; i < out->count; i++) {
            std::cout << out->offsets[i] << ", ";
        }

        std::cout << "lengths: ";
        for (int i = 0; i < out->count; i++) {
            std::cout << out->lengths[i] << ", ";
        }
        std::cout << std::endl;
    #endif

    size_t validity_count = ceil(out->count / 64.0);
    out->validityBuffer = (uint64_t *)malloc(validity_count * sizeof(uint64_t));
    if (!out->validityBuffer) {
        std::cout << "Failed to malloc " << validity_count << " * sizeof(uint64_t)" << std::endl;
        return;
    }
    for (int i = 0; i < validity_count; i++) {
        out->validityBuffer[i] = 0xffffffffffffffff;
    }
}


void debug_words(frovedis::words &in) {
    std::cout << "words char count: " << in.chars.size() << std::endl;
    std::cout << "words starts count: " << in.starts.size() << std::endl;
    std::cout << "words lens count: " << in.lens.size() << std::endl;
    std::cout << "First word starts at: " << in.starts[0] << " length: " << in.lens[0] << " '";

    size_t start = in.starts[0];
    for (int i = 0; i < std::min((long)in.lens[0], 64L); i++) {
        std::cout << (char)in.chars[start + i];
    }
    std::cout << "'" << std::endl;

    std::cout << "Last word " << in.starts.size() - 1 << " starts at: " << in.starts[in.starts.size() -1] << " length[" << in.lens.size() - 1 << "]: " << in.lens[in.lens.size() - 1] << " '";
    start = in.starts[in.starts.size() - 1];
    for (int i = 0; i < std::min((long)in.lens[in.lens.size() - 1], 64L); i++) {
        std::cout << (char)in.chars[start + i];
    }
    std::cout << "'" << std::endl;
}

#define VE_TD_DEFS 1
#endif

