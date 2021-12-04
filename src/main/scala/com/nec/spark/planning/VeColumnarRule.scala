package com.nec.spark.planning

import com.nec.spark.planning.VeColBatchConverters.{SparkToVectorEngine, VectorEngineToSpark}
import org.apache.spark.sql.VeCachePlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command.{CacheTableCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

final class VeColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = {
    case imr @ InMemoryTableScanExec(attributes, predicates, InMemoryRelation(output, cb, oo))
        if cb.serializer
          .isInstanceOf[VeCachedBatchSerializer] && VeCachedBatchSerializer.ShortCircuit =>
      VeShortCircuitPlan(imr)
    case plan =>
      plan.transform {
        case RowToArrowColumnarPlan(ArrowColumnarToRowPlan(child)) => child
        case SparkToVectorEngine(VectorEngineToSpark(child))       => child
      }
  }

}
