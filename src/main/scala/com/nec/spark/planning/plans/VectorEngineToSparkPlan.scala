package com.nec.spark.planning.plans

import com.nec.cmake.ScalaTcpDebug
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.cleanUpIfNotCached
import com.nec.spark.planning.ArrowBatchToUnsafeRows.mapBatchToRow
import com.nec.spark.planning.{SupportsVeColBatch, Tracer}
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VectorEngineToSparkPlan(override val child: SparkPlan)
  extends UnaryExecNode
  with LazyLogging {
  override def supportsColumnar: Boolean = true

  override def doExecute(): RDD[InternalRow] = {
    val tcpDebug = ScalaTcpDebug(VeCompilerConfig.fromSparkConf(sparkContext.getConf))
    val tracer = Tracer.Launched.fromSparkContext(sparkContext)
    doExecuteColumnar().mapPartitions(columnarBatchIterator =>
      tcpDebug
        .toSpanner(tracer)
        .spanIterator("map batch to internal row") {
          columnarBatchIterator.flatMap(mapBatchToRow)
        }
    )
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val tcpDebug = ScalaTcpDebug(VeCompilerConfig.fromSparkConf(sparkContext.getConf))
    val tracer = Tracer.Launched.fromSparkContext(sparkContext)

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { iterator =>
        import SparkCycloneExecutorPlugin._
        lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)

        tcpDebug
          .toSpanner(tracer)
          .spanIterator("map ve batch to arrow columnar batch") {
            iterator
              .map { veColBatch =>
                import OriginalCallingContext.Automatic._

                try {
                  logger.debug(s"Mapping veColBatch ${veColBatch} to arrow...")
                  val res = veColBatch.toArrowColumnarBatch()
                  logger.debug(s"Finished mapping ${veColBatch}")
                  res
                } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
              }
          }
      }
  }

  override def output: Seq[Attribute] = child.output

}
