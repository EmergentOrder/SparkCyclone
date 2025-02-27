package com.nec.spark.planning.plans

import com.nec.arrow.ArrowEncodingSettings
import com.nec.cache.{CycloneCacheBase, DualMode}
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.{DataCleanup, SupportsVeColBatch}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed

case class SparkToVectorEnginePlan(childPlan: SparkPlan)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch {

  override protected def doCanonicalize(): SparkPlan = {
    super.doCanonicalize()
  }

  override def child: SparkPlan = {
    childPlan
  }

  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")

    //      val numInputRows = longMetric("numInputRows")
    //      val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    implicit val arrowEncodingSettings = ArrowEncodingSettings.fromConf(conf)(sparkContext)

    child.execute().mapPartitions { internalRows =>
      import SparkCycloneExecutorPlugin._
      implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
      TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
      import OriginalCallingContext.Automatic._

      DualMode.unwrapPossiblyDualToVeColBatches(
        possiblyDualModeInternalRows = internalRows,
        arrowSchema = CycloneCacheBase.makaArrowSchema(child.output)
      )
    }
  }

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

}
