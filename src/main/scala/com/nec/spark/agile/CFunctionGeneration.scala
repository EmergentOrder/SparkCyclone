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
package com.nec.spark.agile

import com.nec.cmake.TcpDebug
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{
  VeNullableDouble,
  VeNullableFloat,
  VeNullableInt,
  VeNullableLong
}
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.StringProducer.{
  FrovedisCopyStringProducer,
  FrovedisStringProducer,
  ImperativeStringProducer
}
import com.nec.spark.agile.groupby.GroupByOutline
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.UserDefinedVeType
import org.apache.spark.sql.types._

/** Spark-free function evaluation */
object CFunctionGeneration {
  def veType(d: DataType): VeScalarType = {
    d match {
      case DoubleType =>
        VeScalarType.VeNullableDouble
      case IntegerType | DateType =>
        VeScalarType.VeNullableInt
      case x =>
        sys.error(s"unsupported dataType $x")
    }
  }

  trait SortOrdering
  final case object Descending extends SortOrdering
  final case object Ascending extends SortOrdering

  sealed trait CVector {
    def withNewName(str: String): CVector
    def declarePointer: String = s"${veType.cVectorType} *${name}"
    def replaceName(search: String, replacement: String): CVector
    def name: String
    def veType: VeType
  }
  object CVector {
    def apply(name: String, veType: VeType): CVector =
      veType match {
        case VeString        => varChar(name)
        case o: VeScalarType => CScalarVector(name, o)
      }
    def varChar(name: String): CVector = CVarChar(name)
    def double(name: String): CVector = CScalarVector(name, VeScalarType.veNullableDouble)
    def int(name: String): CVector = CScalarVector(name, VeScalarType.veNullableInt)
    def bigInt(name: String): CVector = CScalarVector(name, VeScalarType.VeNullableLong)
  }

  final case class CVarChar(name: String) extends CVector {
    override def veType: VeType = VeString

    override def replaceName(search: String, replacement: String): CVector =
      copy(name = name.replaceAllLiterally(search, replacement))

    override def withNewName(str: String): CVector = copy(name = str)
  }

  final case class CScalarVector(name: String, veType: VeScalarType) extends CVector {
    override def replaceName(search: String, replacement: String): CVector =
      copy(name = name.replaceAllLiterally(search, replacement))

    override def withNewName(str: String): CVector = copy(name = str)
  }

  final case class CExpression(cCode: String, isNotNullCode: Option[String]) {
    def storeTo(outputName: String): CodeLines = isNotNullCode match {
      case None =>
        CodeLines
          .from(
            s"${outputName}->data[i] = ${cCode};",
            s"set_validity($outputName->validityBuffer, i, 1);"
          )
          .indented
      case Some(nullCheck) =>
        CodeLines
          .from(
            s"if( ${nullCheck} ) {",
            CodeLines
              .from(
                s"${outputName}->data[i] = ${cCode};",
                s"set_validity($outputName->validityBuffer, i, 1);"
              )
              .indented,
            "} else {",
            CodeLines.from(s"set_validity($outputName->validityBuffer, i, 0);").indented,
            "}"
          )
          .indented
    }

  }
  final case class CExpressionWithCount(cCode: String, isNotNullCode: Option[String])

  final case class TypedCExpression2(veType: VeScalarType, cExpression: CExpression)
  final case class NamedTypedCExpression(
    name: String,
    veType: VeScalarType,
    cExpression: CExpression
  )
  final case class NamedStringExpression(name: String, stringProducer: StringProducer)

  @SQLUserDefinedType(udt = classOf[UserDefinedVeType])
  sealed trait VeType {
    def containerSize: Int
    def isString: Boolean
    def cVectorType: String
    def makeCVector(name: String): CVector
  }

  object VeType {
    val All: Set[VeType] = Set(VeString) ++ VeScalarType.All
  }

  case object VeString extends VeType {
    override def cVectorType: String = "nullable_varchar_vector"

    override def makeCVector(name: String): CVector = CVector.varChar(name)

    override def isString: Boolean = true

    override def containerSize: Int = 32
  }

  sealed trait VeScalarType extends VeType {
    override def containerSize: Int = 20

    def cScalarType: String

    def cSize: Int

    override def makeCVector(name: String): CVector = CScalarVector(name, this)

    override def isString: Boolean = false
  }

  object VeScalarType {
    val All: Set[VeScalarType] =
      Set(VeNullableDouble, VeNullableFloat, VeNullableInt, VeNullableLong)
    case object VeNullableDouble extends VeScalarType {

      def cScalarType: String = "double"

      def cVectorType: String = "nullable_double_vector"

      override def cSize: Int = 8
    }

    case object VeNullableFloat extends VeScalarType {
      def cScalarType: String = "float"

      def cVectorType: String = "nullable_float_vector"

      override def cSize: Int = 4
    }

    case object VeNullableInt extends VeScalarType {
      def cScalarType: String = "int32_t"

      def cVectorType: String = "nullable_int_vector"

      override def cSize: Int = 4
    }

    case object VeNullableLong extends VeScalarType {
      def cScalarType: String = "int64_t"

      def cVectorType: String = "nullable_bigint_vector"

      override def cSize: Int = 8
    }

    def veNullableDouble: VeScalarType = VeNullableDouble
    def veNullableInt: VeScalarType = VeNullableInt
    def veNullableLong: VeScalarType = VeNullableLong
  }

  /**
   * The reason to use fully generic types is so that we can map them around in future, without having an implementation
   * that interferes with those changes. By 'manipulate' we mean optimize/find shortcuts/etc.
   *
   * By doing this, we flatten the code hierarchy and can now do validation of C behaviors without requiring Spark to be pulled in.
   *
   * This even enables us the possibility to use Frovedis behind the scenes.
   */
  final case class VeProjection[Input, Output](inputs: List[Input], outputs: List[Output])

  final case class VeGroupBy[Input, Group, Output](
    inputs: List[Input],
    groups: List[Group],
    outputs: List[Output]
  )
  sealed trait JoinType
  case object LeftOuterJoin extends JoinType
  case object RightOuterJoin extends JoinType

  final case class VeInnerJoin[Input, LeftKey, RightKey, Output](
    inputs: List[Input],
    leftKey: LeftKey,
    rightKey: RightKey,
    outputs: List[Output]
  )

  final case class OuterJoinOutput[Output](innerJoinOutputs: Output, outerJoinOutputs: Output)

  final case class VeOuterJoin[Input, LeftKey, RightKey, Output](
    inputs: List[Input],
    leftKey: LeftKey,
    rightKey: RightKey,
    outputs: List[OuterJoinOutput[Output]],
    joinType: JoinType
  )
  sealed trait JoinExpression {
    def fold[T](whenProj: CExpression => T): T
  }
  final case class TypedJoinExpression[ScalaType](joinExpression: JoinExpression)

  object JoinExpression {
    final case class JoinProjection(cExpression: CExpression) extends JoinExpression {
      override def fold[T](whenProj: CExpression => T): T = whenProj(cExpression)
    }
    //TODO: We can use that to meld join and aggregate
//    final case class JoinAggregation(aggregation: Aggregation) extends JoinExpression {
//      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenAgg(
//        aggregation
//      )
//    }
  }
  final case class NamedJoinExpression(
    name: String,
    veType: VeScalarType,
    joinExpression: JoinExpression
  )

  final case class TypedGroupByExpression[ScalaType](groupByExpression: GroupByExpression)

  sealed trait GroupByExpression {
    def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T
  }
  object GroupByExpression {
    final case class GroupByProjection(cExpression: CExpression) extends GroupByExpression {
      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenProj(
        cExpression
      )
    }
    final case class GroupByAggregation(aggregation: Aggregation) extends GroupByExpression {
      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenAgg(
        aggregation
      )
    }
  }

  final case class NamedGroupByExpression(
    name: String,
    veType: VeScalarType,
    groupByExpression: GroupByExpression
  )

  trait Aggregation extends Serializable {
    def merge(prefix: String, inputPrefix: String): CodeLines
    def initial(prefix: String): CodeLines
    def partialValues(prefix: String): List[(CScalarVector, CExpression)]
    def iterate(prefix: String): CodeLines
    def compute(prefix: String): CodeLines
    def fetch(prefix: String): CExpression
    def free(prefix: String): CodeLines
  }

  final case class SuffixedAggregation(suffix: String, original: Aggregation) extends Aggregation {
    override def initial(prefix: String): CodeLines = original.initial(s"$prefix$suffix")

    override def iterate(prefix: String): CodeLines = original.iterate(s"$prefix$suffix")

    override def compute(prefix: String): CodeLines = original.compute(s"$prefix$suffix")

    override def fetch(prefix: String): CExpression = original.fetch(s"$prefix$suffix")

    override def free(prefix: String): CodeLines = original.free(s"$prefix$suffix")

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      original.partialValues(s"$prefix$suffix")

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      original.merge(s"$prefix$suffix", s"$inputPrefix$suffix")
  }

  abstract class DelegatingAggregation(val original: Aggregation) extends Aggregation {
    override def initial(prefix: String): CodeLines = original.initial(prefix)

    override def iterate(prefix: String): CodeLines = original.iterate(prefix)

    override def compute(prefix: String): CodeLines = original.compute(prefix)

    override def fetch(prefix: String): CExpression = original.fetch(prefix)

    override def free(prefix: String): CodeLines = original.free(prefix)

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      original.partialValues(prefix)

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      original.merge(prefix, inputPrefix)
  }

  object Aggregation {
    def sum(cExpression: CExpression): Aggregation = new Aggregation {
      override def initial(prefix: String): CodeLines =
        CodeLines.from(s"double ${prefix}_aggregate_sum = 0;")

      override def iterate(prefix: String): CodeLines =
        cExpression.isNotNullCode match {
          case None =>
            CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};")
          case Some(notNullCheck) =>
            CodeLines.from(
              s"if ( ${notNullCheck} ) {",
              CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};").indented,
              "}"
            )
        }

      override def fetch(prefix: String): CExpression =
        CExpression(s"${prefix}_aggregate_sum", None)

      override def free(prefix: String): CodeLines = CodeLines.empty

      override def compute(prefix: String): CodeLines = CodeLines.empty

      override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
        List(
          (
            CScalarVector(s"${prefix}_x", VeScalarType.veNullableDouble),
            CExpression(s"${prefix}_aggregate_sum", None)
          )
        )

      override def merge(prefix: String, inputPrefix: String): CodeLines =
        CodeLines.from(s"${prefix}_aggregate_sum += ${inputPrefix}_x->data[i];")
    }
    def avg(cExpression: CExpression): Aggregation = new Aggregation {
      override def initial(prefix: String): CodeLines =
        CodeLines.from(
          s"double ${prefix}_aggregate_sum = 0;",
          s"long ${prefix}_aggregate_count = 0;"
        )

      override def iterate(prefix: String): CodeLines =
        cExpression.isNotNullCode match {
          case None =>
            CodeLines.from(
              s"${prefix}_aggregate_sum += ${cExpression.cCode};",
              s"${prefix}_aggregate_count += 1;"
            )
          case Some(notNullCheck) =>
            CodeLines.from(
              s"if ( ${notNullCheck} ) {",
              CodeLines
                .from(
                  s"${prefix}_aggregate_sum += ${cExpression.cCode};",
                  s"${prefix}_aggregate_count += 1;"
                )
                .indented,
              "}"
            )
        }

      override def fetch(prefix: String): CExpression =
        CExpression(s"${prefix}_aggregate_sum / ${prefix}_aggregate_count", None)

      override def free(prefix: String): CodeLines = CodeLines.empty

      override def compute(prefix: String): CodeLines = CodeLines.empty

      override def partialValues(prefix: String): List[(CScalarVector, CExpression)] = List(
        (
          CScalarVector(s"${prefix}_aggregate_sum_partial_output", VeScalarType.veNullableDouble),
          CExpression(s"${prefix}_aggregate_sum", None)
        ),
        (
          CScalarVector(s"${prefix}_aggregate_count_partial_output", VeScalarType.veNullableLong),
          CExpression(s"${prefix}_aggregate_count", None)
        )
      )

      override def merge(prefix: String, inputPrefix: String): CodeLines =
        CodeLines.from(
          s"${prefix}_aggregate_sum += ${inputPrefix}_aggregate_sum_partial_output->data[i];",
          s"${prefix}_aggregate_count += ${inputPrefix}_aggregate_count_partial_output->data[i];"
        )
    }
  }

  final case class VeFilter[Data, Condition](
    stringVectorComputations: List[StringHoleEvaluation],
    data: List[Data],
    condition: Condition
  )

  final case class VeSort[Data, Sort](data: List[Data], sorts: List[Sort])

  final case class VeSortExpression(typedExpression: TypedCExpression2, sortOrdering: SortOrdering)
  def allocateFrom(cVector: CVector)(implicit bufferAllocator: BufferAllocator): FieldVector =
    cVector.veType match {
      case VeString =>
        new VarCharVector(cVector.name, bufferAllocator)
      case VeNullableDouble =>
        new Float8Vector(cVector.name, bufferAllocator)
      case VeNullableFloat =>
        new Float8Vector(cVector.name, bufferAllocator)
      case VeNullableInt =>
        new IntVector(cVector.name, bufferAllocator)
      case VeNullableLong =>
        new BigIntVector(cVector.name, bufferAllocator)
    }

  val KeyHeaders = CodeLines.from(
    """#include "transfer-definitions.hpp"""",
    """#include "cyclone.hpp"""",
    "#include <cmath>",
    "#include <bitset>",
    "#include <string>",
    "#include <iostream>",
    "#include <tuple>",
    "#include \"tuple_hash.hpp\"",
    """#include "frovedis/dataframe/join.hpp"""",
    """#include "frovedis/core/set_operations.hpp""""
  )

  final case class CFunction(
    inputs: List[CVector],
    outputs: List[CVector],
    body: CodeLines,
    hasSets: Boolean = false
  ) {
    def toCodeLinesSPtr(functionName: String): CodeLines = CodeLines.from(
      """#include "transfer-definitions.hpp"""",
      """#include "cyclone.hpp"""",
      "#include <cmath>",
      "#include <bitset>",
      "#include <string>",
      "#include <vector>",
      "#include <iostream>",
      "#include <tuple>",
      "#include \"tuple_hash.hpp\"",
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/core/set_operations.hpp"""",
      TcpDebug.conditional.headers,
      toCodeLinesNoHeaderOutPtr2(functionName)
    )

    def toCodeLinesS(functionName: String): CodeLines = CodeLines.from(
      """#include "transfer-definitions.hpp"""",
      """#include "cyclone.hpp"""",
      "#include <cmath>",
      "#include <bitset>",
      "#include <string>",
      "#include <iostream>",
      "#include <tuple>",
      "#include \"tuple_hash.hpp\"",
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/core/set_operations.hpp"""",
      TcpDebug.conditional.headers,
      toCodeLinesNoHeader(functionName)
    )

    def arguments: List[CVector] = inputs ++ outputs

    def toCodeLinesPF(functionName: String): CodeLines = {
      CodeLines.from(
        """#include "transfer-definitions.hpp"""",
        """#include "cyclone.hpp"""",
        "#include <cmath>",
        "#include <bitset>",
        "#include <string>",
        "#include <iostream>",
        TcpDebug.conditional.headers,
        toCodeLinesNoHeader(functionName)
      )
    }

    def toCodeLinesG(functionName: String): CodeLines = {
      CodeLines.from(
        """#include "transfer-definitions.hpp"""",
        """#include "cyclone.hpp"""",
        "#include <cmath>",
        "#include <bitset>",
        "#include <string>",
        "#include <iostream>",
        "#include <tuple>",
        "#include \"tuple_hash.hpp\"",
        TcpDebug.conditional.headers,
        toCodeLinesNoHeader(functionName)
      )
    }
    def toCodeLinesJ(functionName: String): CodeLines = {
      CodeLines.from(
        """#include "transfer-definitions.hpp"""",
        """#include "cyclone.hpp"""",
        "#include <cmath>",
        "#include <bitset>",
        "#include <string>",
        "#include <iostream>",
        """#include "frovedis/dataframe/join.hpp"""",
        """#include "frovedis/core/set_operations.hpp"""",
        TcpDebug.conditional.headers,
        toCodeLinesNoHeader(functionName)
      )
    }

    def toCodeLinesNoHeader(functionName: String): CodeLines = {
      CodeLines.from(
        s"""extern "C" long $functionName(""",
        arguments
          .map { cVector =>
            s"${cVector.veType.cVectorType} *${cVector.name}"
          }
          .mkString(",\n"),
        ") {",
        body.indented,
        "  ",
        "  return 0;",
        "};"
      )
    }

    def toCodeLinesHeaderPtr(functionName: String): CodeLines = {
      CodeLines.from(KeyHeaders, toCodeLinesNoHeaderOutPtr(functionName))
    }

    def toCodeLinesNoHeaderOutPtr(functionName: String): CodeLines = {
      CodeLines.from(
        s"""extern "C" long $functionName(""", {
          inputs
            .map { cVector =>
              s"${cVector.veType.cVectorType} **${cVector.name}"
            } ++ { if (hasSets) List("int *sets") else Nil } ++
            outputs
              .map { cVector =>
                s"${cVector.veType.cVectorType} **${cVector.name}"
              }
        }
          .mkString(",\n"),
        ") {",
        body.indented,
        "  ",
        "  return 0;",
        "};"
      )
    }

    def toCodeLinesNoHeaderOutPtr2(functionName: String): CodeLines = {
      CodeLines.from(
        s"""extern "C" long $functionName(""", {
          List(
            inputs
              .map { cVector =>
                s"${cVector.veType.cVectorType} **${cVector.name}_m"
              },
            if (hasSets) List("int *sets") else Nil,
            outputs
              .map { cVector =>
                s"${cVector.veType.cVectorType} **${cVector.name}_mo"
              }
          ).flatten
        }
          .mkString(",\n"),
        ") {",
        CodeLines
          .from(
            CodeLines.debugHere,
            inputs.map { cVector =>
              CodeLines.from(
                s"${cVector.veType.cVectorType}* ${cVector.name} = ${cVector.name}_m[0];"
              )
            },
            CodeLines.debugHere,
            outputs.map { cVector =>
              CodeLines.from(
                s"${cVector.veType.cVectorType}* ${cVector.name} = (${cVector.veType.cVectorType} *)malloc(sizeof(${cVector.veType.cVectorType}));",
                s"*${cVector.name}_mo = ${cVector.name};"
              )
            },
            CodeLines.debugHere,
            body
          )
          .indented,
        "  ",
        "  return 0;",
        "};"
      )
    }
  }

  def renderSort(sort: VeSort[CScalarVector, VeSortExpression]): CFunction = {

    val sortOutput = sort.data.map { case CScalarVector(name, veType) =>
      CScalarVector(name.replaceAllLiterally("input", "output"), veType)
    }
    val sortingTypes = sort.sorts
      .flatMap(sortExpression =>
        List(
          (sortExpression.typedExpression.veType.cScalarType, sortExpression.sortOrdering),
          ("int", sortExpression.sortOrdering)
        )
      )
    val sortingTuple = sort.sorts
      .flatMap(veScalar => List(veScalar.typedExpression.veType.cScalarType, "int"))
      .mkString("std::tuple<", ", ", ">")
    CFunction(
      inputs = sort.data,
      outputs = sortOutput,
      body = CodeLines.from(
        sortOutput.map { case CScalarVector(outputName, outputVeType) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->validityBuffer = (uint64_t *) malloc(ceil($outputName->count / 64.0) * sizeof(uint64_t));",
            s"$outputName->data = (${outputVeType.cScalarType}*) malloc($outputName->count * sizeof(${outputVeType.cScalarType}));"
          )
        },
        "// create an array of indices, which by default are in order, but afterwards are out of order.",
        "std::vector<size_t> idx(input_0->count);",
        s"std::vector<${sortingTuple}> sorting_vec(input_0->count);",
        "for (size_t i = 0; i < input_0->count; i++) {",
        "idx[i] = i;",
        s"sorting_vec[i] = ${sortingTuple}${sort.sorts
          .flatMap {
            case VeSortExpression(TypedCExpression2(dataType, CExpression(cCode, Some(notNullCode))), _) =>
              List(cCode, notNullCode)
            case VeSortExpression(TypedCExpression2(dataType, CExpression(cCode, None)), _) =>
              List(cCode, 1)

          }
          .mkString("(", ",", ")")};",
        "}",
        sortingTypes.zipWithIndex.reverse.map { case ((dataType, order), idx) =>
          CodeLines.from(
            "{",
            CodeLines
              .from(
                s"std::vector<${dataType}> temp(input_0->count);",
                "for(long i = 0; i < input_0->count; i++) {",
                CodeLines.from(s"temp[i] = std::get<${idx}>(sorting_vec[idx[i]]);").indented,
                "}",
                order match {
                  case Ascending => "frovedis::radix_sort(temp.data(), idx.data(), temp.size());"
                  case Descending =>
                    "frovedis::radix_sort_desc(temp.data(), idx.data(), temp.size());"
                }
              )
              .indented,
            "}"
          )
        },
        "// prevent deallocation of input vector -- it is deallocated by the caller",
        s"for(int i = 0; i < input_0->count; i++) {",
        sort.data.zip(sortOutput).map {
          case (CScalarVector(inName, veType), CScalarVector(outputName, _)) =>
            CodeLines
              .from(
                s"if(check_valid(${inName}->validityBuffer, idx[i])) {",
                CodeLines
                  .from(
                    s"$outputName->data[i] = $inName->data[idx[i]];",
                    s"set_validity($outputName->validityBuffer, i, 1);"
                  )
                  .indented,
                "} else {",
                CodeLines.from(s"set_validity($outputName->validityBuffer, i, 0);").indented,
                "}"
              )
              .indented
        },
        "}"
      )
    )
  }

  def renderFilter(filter: VeFilter[CVector, CExpression]): CFunction = {
    val filterOutput = filter.data.map {
      case CScalarVector(name, veType) =>
        CScalarVector(name.replaceAllLiterally("input", "output"), veType)
      case CVarChar(name) =>
        CVarChar(name.replaceAllLiterally("input", "output"))
    }

    CFunction(
      inputs = filter.data,
      outputs = filterOutput,
      body = CodeLines.from(
        filter.stringVectorComputations.distinct.map(_.computeVector),
        s"std::vector<size_t> matching_ids;",
        s"for ( long i = 0; i < input_0->count; i++) {",
        CodeLines
          .from(filter.condition.isNotNullCode match {
            case None =>
              CodeLines.from(
                s"if ( ${filter.condition.cCode} ) {",
                CodeLines.from("matching_ids.push_back(i);").indented,
                "}"
              )
            case Some(notNullCondition) =>
              CodeLines.from(
                s"if ( (${notNullCondition} && ${filter.condition.cCode}) ) {",
                CodeLines.from("matching_ids.push_back(i);").indented,
                "}"
              )
          })
          .indented,
        "}",
        filter.stringVectorComputations.distinct.map(_.deallocData),
        filter.data.zipWithIndex.map {
          case (cv @ CVarChar(name), idx) =>
            val varName = cv.replaceName("input", "output").name
            val fp: FrovedisStringProducer =
              FrovedisCopyStringProducer(name)
            CodeLines.from(
              fp.init(varName, "matching_ids.size()"),
              "for ( int g = 0; g < matching_ids.size(); g++ ) {",
              CodeLines.from("int i = matching_ids[g];", fp.produce(varName, "g")).indented,
              "}",
              fp.complete(varName)
            )
          case (cVector @ CScalarVector(_, tpe), idx) =>
            val varName = cVector.replaceName("input", "output").name
            CodeLines.from(
              CodeLines.debugHere,
              GroupByOutline.initializeScalarVector(
                veScalarType = tpe,
                variableName = varName,
                countExpression = s"matching_ids.size()"
              ),
              "for ( int o = 0; o < matching_ids.size(); o++ ) {",
              CodeLines
                .from(
                  "int i = matching_ids[o];",
                  s"if(check_valid(${cVector.name}->validityBuffer, i)) {",
                  CodeLines
                    .from(
                      s"${varName}->data[o] = ${cVector.name}->data[i];",
                      s"set_validity($varName->validityBuffer, o, 1);"
                    )
                    .indented,
                  "} else {",
                  CodeLines.from(s"set_validity($varName->validityBuffer, o, 0);").indented,
                  "}"
                )
                .indented,
              "}"
            )
        }
      )
    )
  }

  def renderProjection(
    veDataTransformation: VeProjection[
      CVector,
      Either[NamedStringExpression, NamedTypedCExpression]
    ]
  ): CFunction = CFunction(
    inputs = veDataTransformation.inputs,
    outputs = veDataTransformation.outputs.zipWithIndex.map {
      case (Right(NamedTypedCExpression(outputName, veType, _)), idx) =>
        CScalarVector(outputName, veType)
      case (Left(NamedStringExpression(name, _)), idx) =>
        CVarChar(name)
    },
    body = CodeLines.from(
      CodeLines.debugHere,
      veDataTransformation.outputs.zipWithIndex.map {
        case (Right(NamedTypedCExpression(outputName, veType, _)), idx) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->data = (${veType.cScalarType}*) malloc($outputName->count * sizeof(${veType.cScalarType}));",
            s"$outputName->validityBuffer = (uint64_t *) malloc(ceil($outputName->count / 64.0) * sizeof(uint64_t));"
          )
        case (Left(NamedStringExpression(name, stringProducer: ImperativeStringProducer)), idx) =>
          StringProducer
            .produceVarChar(
              inputCount = "input_0->count",
              outputName = name,
              stringProducer = stringProducer
            )
            .block
        case (Left(NamedStringExpression(name, stringProducer: FrovedisStringProducer)), idx) =>
          StringProducer
            .produceVarChar(
              inputCount = "input_0->count",
              outputName = name,
              stringProducer = stringProducer,
              outputCount = "input_0->count",
              outputIdx = "i"
            )
            .block
      },
      CodeLines.debugHere,
      "for ( long i = 0; i < input_0->count; i++ ) {",
      veDataTransformation.outputs.zipWithIndex
        .map {
          case (Right(NamedTypedCExpression(outputName, veType, cExpr)), idx) =>
            cExpr.isNotNullCode match {
              case None =>
                CodeLines.from(
                  s"""$outputName->data[i] = ${cExpr.cCode};""",
                  s"set_validity($outputName->validityBuffer, i, 1);"
                )
              case Some(notNullCheck) =>
                CodeLines.from(
                  s"if ( $notNullCheck ) {",
                  s"""  $outputName->data[i] = ${cExpr.cCode};""",
                  s"  set_validity($outputName->validityBuffer, i, 1);",
                  "} else {",
                  s"  set_validity($outputName->validityBuffer, i, 0);",
                  "}"
                )
            }
          case (Left(_), _) =>
            // already produced for string, because produceVarChar does everything
            CodeLines.empty
        }
        .map(_.indented),
      "}"
    )
  )

  def renderInnerJoin(
    veInnerJoin: VeInnerJoin[CVector, TypedCExpression2, TypedCExpression2, NamedJoinExpression]
  ): CFunction = {

    CFunction(
      inputs = veInnerJoin.inputs,
      outputs = veInnerJoin.outputs.zipWithIndex.map {
        case (NamedJoinExpression(outputName, veType, _), idx) =>
          CScalarVector(outputName, veType)
      },
      body = CodeLines.from(
        s"std::vector <${veInnerJoin.leftKey.veType.cScalarType}> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <${veInnerJoin.rightKey.veType.cScalarType}> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        s"for(int i = 0; i < ${veInnerJoin.leftKey.cExpression.cCode
          .replace("data[i]", "count")}; i++) { ",
        CodeLines
          .from(
            s"left_vec.push_back(${veInnerJoin.leftKey.cExpression.cCode});",
            "left_idx.push_back(i);"
          )
          .indented,
        "}",
        s"for(int i = 0; i < ${veInnerJoin.rightKey.cExpression.cCode
          .replace("data[i]", "count")}; i++) { ",
        CodeLines
          .from(
            s"right_vec.push_back(${veInnerJoin.rightKey.cExpression.cCode});",
            "right_idx.push_back(i);"
          )
          .indented,
        "}",
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        s"frovedis::equi_join<${veInnerJoin.leftKey.veType.cScalarType}>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);",
        "long validityBuffSize = ceil(left_out.size() / 64.0);",
        veInnerJoin.outputs.map { case NamedJoinExpression(outputName, veType, joinExpression) =>
          joinExpression.fold(whenProj =
            _ =>
              CodeLines.from(
                s"${outputName}->data = (${veType.cScalarType}*) malloc(left_out.size() * sizeof(${veType.cScalarType}));",
                s"${outputName}->validityBuffer = (uint64_t *) malloc(validityBuffSize * sizeof(uint64_t));"
              )
          )
        },
        "for(int i = 0; i < left_out.size(); i++) { ",
        veInnerJoin.outputs.map { case NamedJoinExpression(outputName, veType, joinExpression) =>
          joinExpression.fold(ce => ce) match {
            case ex =>
              ex.isNotNullCode match {
                case None =>
                  CodeLines
                    .from(
                      s"${outputName}->data[i] = ${ex.cCode};",
                      s"set_validity($outputName->validityBuffer, i, 1);"
                    )
                    .indented
                case Some(nullCheck) =>
                  CodeLines
                    .from(
                      s"if( ${nullCheck} ) {",
                      CodeLines
                        .from(
                          s"${outputName}->data[i] = ${ex.cCode};",
                          s"set_validity($outputName->validityBuffer, i, 1);"
                        )
                        .indented,
                      "} else {",
                      CodeLines.from(s"set_validity($outputName->validityBuffer, i, 0);").indented,
                      "}"
                    )
                    .indented
              }
          }
        },
        "}",
        veInnerJoin.outputs.map { case NamedJoinExpression(outputName, veType, joinExpression) =>
          CodeLines.from(s"${outputName}->count = left_out.size();")
        }
      )
    )
  }

  def renderOuterJoin(
    veOuterJoin: VeOuterJoin[CVector, TypedCExpression2, TypedCExpression2, NamedJoinExpression]
  ): CFunction = {

    CFunction(
      inputs = veOuterJoin.inputs,
      outputs = veOuterJoin.outputs.zipWithIndex.map {
        case (OuterJoinOutput(NamedJoinExpression(outputName, veType, _), _), idx) =>
          CScalarVector(outputName, veType)
      },
      body = CodeLines.from(
        s"std::vector <std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        CodeLines
          .from(
            veOuterJoin.leftKey.cExpression.isNotNullCode match {
              case Some(notNullCode) =>
                CodeLines.from(
                  s"""for(int i =0; i < ${veOuterJoin.leftKey.cExpression.cCode
                    .replace("data[i]", "count")}; i++) {""",
                  CodeLines
                    .from(
                      "left_idx.push_back(i);",
                      s"if( ${notNullCode}) {",
                      CodeLines
                        .from(
                          s"left_vec.push_back(std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>(${veOuterJoin.leftKey.cExpression.cCode}, 1));"
                        )
                        .indented,
                      "} else {",
                      CodeLines
                        .from(
                          s"left_vec.push_back(std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>(${veOuterJoin.leftKey.cExpression.cCode}, 0));"
                        )
                        .indented,
                      "}"
                    )
                    .indented,
                  "}"
                )
              case None =>
                CodeLines
                  .from(
                    s"""for(int i =0; i < ${veOuterJoin.leftKey.cExpression.cCode
                      .replace("data[i]", "count")}; i++) {""",
                    CodeLines
                      .from(
                        "left_idx.push_back(i);",
                        s"left_vec.push_back(std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>(${veOuterJoin.leftKey.cExpression.cCode}, 1));"
                      )
                      .indented,
                    "}"
                  )
                  .indented
            },
            veOuterJoin.rightKey.cExpression.isNotNullCode match {
              case Some(notNullCode) =>
                CodeLines.from(
                  s"""for(int i =0; i < ${veOuterJoin.rightKey.cExpression.cCode
                    .replace("data[i]", "count")}; i++) {""",
                  CodeLines
                    .from(
                      "right_idx.push_back(i);",
                      s"if( ${notNullCode}) {",
                      CodeLines
                        .from(
                          s"right_vec.push_back(std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>(${veOuterJoin.rightKey.cExpression.cCode}, 1));"
                        )
                        .indented,
                      "} else {",
                      CodeLines
                        .from(
                          s"right_vec.push_back(std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>(${veOuterJoin.rightKey.cExpression.cCode}, 0));"
                        )
                        .indented,
                      "}"
                    )
                    .indented,
                  "}"
                )
              case None =>
                CodeLines.from(
                  s"""for(int i =0; i < ${veOuterJoin.rightKey.cExpression.cCode
                    .replace("data[i]", "count")}; i++) {""",
                  CodeLines
                    .from(
                      "right_idx.push_back(i);",
                      s"right_vec.push_back(std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>(${veOuterJoin.rightKey.cExpression.cCode}, 1));"
                    )
                    .indented,
                  "}"
                )
            }
          )
          .indented,
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        veOuterJoin.joinType match {
          case LeftOuterJoin =>
            CodeLines.from(
              s"std::vector<size_t> outer_idx = frovedis::outer_equi_join<std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);"
            )
          case RightOuterJoin =>
            CodeLines.from(
              s"std::vector<size_t> outer_idx = frovedis::outer_equi_join<std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>>(right_vec, right_idx, left_vec, left_idx, right_out, left_out);"
            )
        },
        List("long validityBuffSize = ceil((left_out.size() + outer_idx.size()) / 64.0);"),
        veOuterJoin.outputs.map {
          case OuterJoinOutput(NamedJoinExpression(outputName, veType, joinExpression), _) =>
            joinExpression.fold(whenProj =
              _ =>
                CodeLines.from(
                  s"${outputName}->data = (${veType.cScalarType}*) malloc((left_out.size() + outer_idx.size()) * sizeof(${veType.cScalarType}));",
                  s"${outputName}->validityBuffer = (uint64_t *) malloc(validityBuffSize * sizeof(uint64_t));"
                )
            )
        },
        "for(int i = 0; i < left_out.size(); i++) { ",
        veOuterJoin.outputs.map {
          case OuterJoinOutput(NamedJoinExpression(outputName, veType, joinExpression), _) =>
            joinExpression.fold(ce => ce) match {
              case ex =>
                ex.isNotNullCode match {
                  case None =>
                    CodeLines
                      .from(
                        s"${outputName}->data[i] = ${ex.cCode};",
                        s"set_validity($outputName->validityBuffer, i, 1);"
                      )
                      .indented
                  case Some(nullCheck) =>
                    CodeLines
                      .from(
                        s"if( ${nullCheck} ) {",
                        s"${outputName}->data[i] = ${ex.cCode};",
                        s"set_validity($outputName->validityBuffer, i, 1);",
                        "} else {",
                        s"set_validity($outputName->validityBuffer, i, 0);",
                        "}"
                      )
                      .indented
                }
            }
        },
        "}",
        CodeLines.from(
          "#pragma _NEC ivdep",
          "for (int i = left_out.size(); i < (left_out.size() + outer_idx.size()); i++) {",
          "int idx = i - left_out.size();"
        ),
        veOuterJoin.outputs.map {
          case OuterJoinOutput(_, NamedJoinExpression(outputName, veType, joinExpression)) => {
            joinExpression.fold(ce => ce) match {
              case ex =>
                ex.isNotNullCode match {
                  case None =>
                    CodeLines
                      .from(
                        s"${outputName}->data[i] = ${ex.cCode};",
                        s"set_validity($outputName->validityBuffer, i, 1);"
                      )
                      .indented
                  case Some(nullCheck) =>
                    CodeLines
                      .from(
                        s"if( ${nullCheck} ) {",
                        s"${outputName}->data[i] = ${ex.cCode};",
                        s"set_validity($outputName->validityBuffer, i, 1);",
                        "} else {",
                        s"set_validity($outputName->validityBuffer, i, 0);",
                        "}"
                      )
                      .indented
                }
            }
          }

        },
        CodeLines.from("}"),
        veOuterJoin.outputs.map {
          case OuterJoinOutput(NamedJoinExpression(outputName, veType, joinExpression), _) =>
            CodeLines.from(s"${outputName}->count = left_out.size() + outer_idx.size();")
        }
      )
    )
  }

  final case class StringGrouping(name: String)

  final case class NamedStringProducer(name: String, stringProducer: StringProducer)

}
