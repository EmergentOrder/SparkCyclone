package com.nec.spark.planning

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

object SparkPortingUtils {

  implicit class PortedSparkPlan(sparkPlan: SparkPlan) {
    def supportsColumnar: Boolean = false
  }

  case class ResourceInformation(name: String, resources: Array[String])


}
