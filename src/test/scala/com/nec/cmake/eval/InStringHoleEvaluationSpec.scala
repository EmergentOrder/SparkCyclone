package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector, VeScalarType}
import com.nec.spark.agile.StringHole.StringHoleEvaluation.InStringHoleEvaluation
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.RichVectors.RichIntVector
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class InStringHoleEvaluationSpec extends AnyWordSpec {
  implicit class EvalOps(evaluation: InStringHoleEvaluation) {
    def execute(input: List[String]): List[Int] = {
      val code = CodeLines.from(
        evaluation.computeVector,
        GroupByOutline.initializeScalarVector(VeScalarType.veNullableInt, "bools", "strings->count"),
        CodeLines.forLoop("i", "strings->count") {
          GroupByOutline.storeTo("bools", evaluation.fetchResult, "i")
        },
        evaluation.deallocData,
        "return 0;"
      )

      val cLib = CMakeBuilder.buildCLogging(
        CFunction(
          List(CVector.varChar("strings")),
          List(CVector.int("bools")),
          code
        ).toCodeLinesG("test").cCode
      )

      val nativeInterface = new CArrowNativeInterface(cLib.toString)

      WithTestAllocator { implicit allocator =>
        ArrowVectorBuilders.withArrowStringVector(input) { inVec =>
          ArrowVectorBuilders.withDirectIntVector(Seq.empty) { outVec =>
            nativeInterface.callFunction(
              name = "test",
              inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
              outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
            )
            outVec.toList
          }
        }
      }
    }
  }

  "InStringHoleEvaluation" should {
    "correctly filter out input set" in {
      val list = List("Dog", "Cat", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
      val toMatchList = List("Dog", "Cat", "Fox")
      val expected = list.collect {
        case elem if (toMatchList.contains(elem)) => 1
        case _                                    => 0
      }

      InStringHoleEvaluation("strings", toMatchList).execute(list) should be (expected)
    }

    "correctly filter out input set if no matches are preset" in {
      val list = List("Dog", "Cat", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
      val toMatchList = List("not", "here", "Has")
      val expected = list.collect {
        case elem if (toMatchList.contains(elem)) => 1
        case _                                    => 0
      }

      InStringHoleEvaluation("strings", toMatchList).execute(list) should be (expected)
    }

    "correctly filter out input set if all words match" in {
      val list = List("Dog", "Cat", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
      val toMatchList = List("Dog", "Cat", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
      val expected = list.collect {
        case elem if (toMatchList.contains(elem)) => 1
        case _                                    => 0
      }

      InStringHoleEvaluation("strings", toMatchList).execute(list) should be (expected)
    }

    "correctly filter out input set with more complex matches" in {
      val list = List("Fox", "Dog", "CatFox", "SparkCyclone")
      val toMatchList = List("Dog", "Cat", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
      val expected = list.collect {
        case elem if (toMatchList.contains(elem)) => 1
        case _                                    => 0
      }

      InStringHoleEvaluation("strings", toMatchList).execute(list) should be (expected)
    }

    "correctly filter out input set when match words contain spaces or other non-alphanumeric characters" in {
      val delim = Random.shuffle(1.to(255).filter(!_.toChar.isLetterOrDigit)).head.toChar
      val list = List(s"Cat${delim}Dog", "Cow", "Hotel", "Cyclone", "Spark", "Brown", "Fox")
      val toMatchList = List(s"Cat${delim}Dog", "Fox")
      val expected = list.collect {
        case elem if (toMatchList.contains(elem)) => 1
        case _                                    => 0
      }

      InStringHoleEvaluation("strings", toMatchList).execute(list) should be (expected)
    }
  }
}
