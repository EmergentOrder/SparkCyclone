package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders

import java.nio.file.Paths
import java.time.Instant
import com.nec.aurora.Aurora
import com.nec.arrow.functions.Join._
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import org.scalatest.freespec.AnyFreeSpec
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

import java.nio.file.Files

final class JoinVeSpec extends AnyFreeSpec {
  "We can join two lists" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectories(veBuildPath)
    val oPath = veBuildPath.resolve("join.so")
    val theCommand = List(
      "nc++",
      "-O3",
      "-fpic",
      "-pthread",
      "-o",
      oPath.toString,
      "-I./src/main/resources/com/nec/arrow/functions/cpp",
      "-I./src/main/resources/com/nec/arrow/functions/cpp/frovedis",
      "-I./src/main/resources/com/nec/arrow/functions/cpp/frovedis/dataframe",
      "-shared",
      "-I./src/main/resources/com/nec/arrow/functions",
      "-I./src/main/resources/com/nec/arrow/",
      "-xc++",
      "./src/main/resources/com/nec/arrow/functions/cpp/joiner.cc"
    )

    import scala.sys.process._
    info(theCommand.!!)

    val proc = Aurora.veo_proc_create(0)
    val (sorted, expectedSorted) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {

          val alloc = new RootAllocator(Integer.MAX_VALUE)
          val outVector = new Float8Vector("value", alloc)
          val firstColumn: Seq[Double] = Seq(5, 1, 2, 34, 6)
          val secondColumn: Seq[Double] = Seq(100, 15, 92, 331, 49)
          val firstColumnKeys: Seq[Int] = Seq(1, 2, 3, 4, 5)
          val secondColumnKeys: Seq[Int] = Seq(4, 2, 5, 200, 800)
          val lib: Long = Aurora.veo_load_library(proc, oPath.toString)
          ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
            ArrowVectorBuilders.withDirectFloat8Vector(secondColumn) { secondColumnVec =>
              ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
                ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
                  runOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))(
                    firstColumnVec,
                    secondColumnVec,
                    firstKeysVec,
                    secondKeysVec,
                    outVector
                  )
                  val res = (0 until outVector.getValueCount)
                    .map(i => outVector.get(i))
                    .toList
                    .splitAt(outVector.getValueCount / 2)
                  val joinResult = res._1.zip(res._2)
                  (
                    joinResult,
                    joinJVM(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec)
                  )
                }
              }
            }
          }

        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }
}
