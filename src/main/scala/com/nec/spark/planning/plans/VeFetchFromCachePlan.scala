package com.nec.spark.planning.plans

import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.spark.planning.{SupportsVeColBatch, VeCachedBatchSerializer}
import com.nec.ve.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeFetchFromCachePlan(child: SparkPlan) extends UnaryExecNode with SupportsVeColBatch with LazyLogging {
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .executeColumnar()
    .map(cb => {
      logger.debug(s"Mapping ColumnarBatch ${cb} to VE")
      val res = VeCachedBatchSerializer.unwrapBatch(cb)
      logger.debug(s"Finished mapping ColumnarBatch ${cb} to VE: ${res}")
      res
    })

  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
  override def dataCleanup: SupportsVeColBatch.DataCleanup = DataCleanup.NoCleanup
}
