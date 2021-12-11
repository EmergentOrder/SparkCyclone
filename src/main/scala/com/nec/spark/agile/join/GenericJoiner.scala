package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline

object GenericJoiner {
  def produce: CodeLines = {
    val x_a_words = "x_a_words"
    val x_a_cwords = "x_a_cwords"
    val y_a_words = "y_a_words"
    val y_a_cwords = "y_a_cwords"
    val x_a = "x_a"
    val x_b = "x_b"
    val x_c = "x_c"
    val y_a = "y_a"
    val y_b = "y_b"
    val y_c = "y_c"
    val o_a = "o_a"
    val o_b = "o_b"
    val o_c = "o_c"
    CodeLines.from(
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/dataframe/join.cc"""",
      """#include "frovedis/text/words.hpp"""",
      """#include "frovedis/text/words.cc"""",
      """#include "frovedis/text/dict.hpp"""",
      """#include "frovedis/text/dict.cc"""",
      """#include <iostream>""",
      """#include <vector>""",
      """#include <cmath>""",
      """#ifdef DEBUG""",
      """template<typename T>""",
      """void print_vec(char *name, std::vector<T> a) {""",
      CodeLines
        .from(
          """std::cout << name << " = [";""",
          """char *comma = "";""",
          """for (int i = 0; i < a.size(); i++) {""",
          """std::cout << comma << a[i];""",
          """comma = ",";""",
          """}""",
          """std::cout << "]" << std::endl;"""
        )
        .indented,
      """}""",
      """#endif""",
      """""",
      """extern "C" long adv_join(""",
      s"""nullable_varchar_vector *${x_a},""",
      s"""nullable_bigint_vector *${x_b},""",
      s"""nullable_int_vector *${x_c},""",
      s"""nullable_varchar_vector *${y_a},""",
      s"""nullable_bigint_vector *${y_b},""",
      s"""nullable_double_vector *${y_c},""",
      s"""nullable_varchar_vector *${o_a},""",
      s"""nullable_int_vector *${o_b},""",
      s"""nullable_double_vector *${o_c})""",
      """{""",
      CodeLines
        .from(
          s"""frovedis::words ${x_a_words} = varchar_vector_to_words(${x_a});""",
          s"""frovedis::words ${y_a_words} = varchar_vector_to_words(${y_a});""",
          s"""frovedis::compressed_words ${x_a_cwords} = frovedis::make_compressed_words(${x_a_words});""",
          s"""frovedis::compressed_words ${y_a_cwords} = frovedis::make_compressed_words(${y_a_words});""",
          s"""frovedis::dict left_dict = frovedis::make_dict(${x_a_words});""",
          s"""frovedis::dict right_dict = frovedis::make_dict(${y_a_words});""",
          """""",
          """// First = => a1 = a2""",
          s"""std::vector<size_t> a1 = left_dict.lookup(${x_a_cwords});""",
          """std::vector<size_t> a1_idx(a1.size());""",
          CodeLines.forLoop("i", "a1.size()")(CodeLines.from("""a1_idx[i] = i;""")),
          s"""std::vector<size_t> a2 = left_dict.lookup(${y_a_cwords});""",
          """std::vector<size_t> a2_idx(a2.size());""",
          CodeLines.forLoop("i", "a2.size()")(CodeLines.from("""a2_idx[i] = i;""")),
          """std::vector<size_t> a1_out;""",
          """std::vector<size_t> a2_out;""",
          """frovedis::equi_join(a2, a2_idx, a1, a1_idx, a2_out, a1_out);""",
          s"""std::vector<int64_t> b1(${x_b}->count);""",
          s"""std::vector<size_t> b1_idx(${x_b}->count);""",
          CodeLines.forLoop(counterName = "i", until = s"${x_b}->count")(
            CodeLines.from(s"""b1[i] = ${x_b}->data[i];""", """b1_idx[i] = i;""")
          ),
          s"""std::vector<int64_t> b2(${y_b}->count);""",
          s"""std::vector<size_t> b2_idx(${y_b}->count);""",
          CodeLines.forLoop(counterName = "i", until = s"${y_b}->count")(
            CodeLines.from(s"""b2[i] = ${y_b}->data[i];""", """b2_idx[i] = i;""")
          ),
          """std::vector<size_t> b1_out;""",
          """std::vector<size_t> b2_out;""",
          """frovedis::equi_join(b2, b2_idx, b1, b1_idx, b2_out, b1_out);""",
          """std::vector<size_t> conj_x;""",
          """std::vector<size_t> conj_y;""",
          CodeLines.forLoop(counterName = "i", until = "a1_out.size()")(
            CodeLines.forLoop(counterName = "j", until = "b1_out.size()")(
              CodeLines.ifStatement(condition = """a1_out[i] == b1_out[j]""")(
                CodeLines.ifStatement(condition = """a2_out[i] == b2_out[j]""")(
                  CodeLines
                    .from("""conj_x.push_back(a1_out[i]);""", """conj_y.push_back(a2_out[i]);""")
                )
              )
            )
          ),
          """frovedis::words result = left_dict.index_to_words(a2);""",
          s"""words_to_varchar_vector(result, ${o_a});""",
          GroupByOutline
            .initializeScalarVector(VeScalarType.VeNullableInt, s"${o_b}", "conj_x.size()"),
          CodeLines.forLoop(counterName = "i", until = "conj_x.size()")(
            CodeLines
              .from(
                s"""${o_b}->data[i] = ${x_c}->data[conj_x[i]];""",
                s"""set_validity(${o_b}->validityBuffer, i, check_valid(${x_c}->validityBuffer, conj_x[i]));"""
              )
          ),
          GroupByOutline.initializeScalarVector(
            veScalarType = VeScalarType.VeNullableDouble,
            variableName = s"${o_c}",
            countExpression = "conj_y.size()"
          ),
          CodeLines.forLoop(counterName = "i", until = "conj_y.size()")(
            CodeLines
              .from(
                s"""${o_c}->data[i] = ${y_c}->data[conj_y[i]];""",
                s"""set_validity(${o_c}->validityBuffer, i, check_valid(${y_c}->validityBuffer, conj_y[i]));"""
              )
          ),
          """return 0;"""
        )
        .indented,
      """}"""
    )
  }
}
