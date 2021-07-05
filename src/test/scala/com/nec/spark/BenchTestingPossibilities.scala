package com.nec.spark

import com.nec.cmake.CNativeEvaluator
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.NativeCsvExec.NativeCsvStrategy
import com.nec.spark.planning.VERewriteStrategy
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.planning.simplesum.JoinPlanSpec
import com.nec.testing.SampleSource
import com.nec.testing.SampleSource.SampleColA
import com.nec.testing.SampleSource.SampleColB
import com.nec.testing.Testing
import com.nec.testing.Testing.DataSize
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.internal.StaticSQLConf.CODEGEN_COMMENTS

object BenchTestingPossibilities {

  sealed trait VeColumnMode {
    final override def toString: String = label
    def offHeapEnabled: Boolean
    def compressed: Boolean
    def label: String
  }
  object VeColumnMode {
    val All = List(OffHeapDisabled, OffHeapEnabledUnCompressed, OffHeapEnabledCompressed)
    case object OffHeapDisabled extends VeColumnMode {
      def offHeapEnabled: Boolean = false
      def compressed: Boolean = false
      def label: String = "OffHeapDisabled"
    }
    case object OffHeapEnabledUnCompressed extends VeColumnMode {
      def offHeapEnabled: Boolean = true
      def compressed: Boolean = false
      def label: String = "OffHeapEnabledUncompressed"
    }
    case object OffHeapEnabledCompressed extends VeColumnMode {
      def offHeapEnabled: Boolean = true
      def compressed: Boolean = true
      def label: String = "OffHeapEnabledCompressed"
    }
  }

  sealed trait CsvStrategy {
    def expectedString: Option[String]
    final override def toString: String = label
    def label: String
    def isNative: Boolean
  }
  object CsvStrategy {
    case object NativeCsv extends CsvStrategy {
      override def label: String = "NativeCsv"
      override def isNative: Boolean = true
      override def expectedString: Option[String] = Some("NativeCsv")
    }
    case object NormalCsv extends CsvStrategy {
      override def label: String = "NormalCsv"
      override def isNative: Boolean = false
      override def expectedString: Option[String] = None
    }
    val All: List[CsvStrategy] = List(NativeCsv, NormalCsv)
  }

  import com.eed3si9n.expecty.Expecty.assert
  final case class SimpleSql(
    sql: String,
    expectedResult: (Double, Double),
    source: SampleSource,
    testingTarget: TestingTarget,
    offHeapMode: Option[VeColumnMode],
    csvStrategy: CsvStrategy
  ) extends Testing {

    type Result = (Double, Double)

    override def verifyResult(result: List[Result]): Unit = {
      assert(result == List(expectedResult))
    }

    def expectedStrings: List[String] =
      testingTarget.expectedString.toList ++ csvStrategy.expectedString.toList ++ {
        if (testingTarget.isNative) List("CEvaluation") else Nil
      }

    override def prepareInput(sparkSession: SparkSession, dataSize: DataSize): Dataset[Result] = {
      source.generate(sparkSession, dataSize)
      import sparkSession.sqlContext.implicits._
      val dataSet = sparkSession.sql(sql).as[Result]

      val planString = dataSet.queryExecution.executedPlan.toString()
      expectedStrings.foreach { expStr =>
        assert(
          planString.contains(expStr),
          s"Expected the plan to contain '$expStr', but it didn't"
        )
      }

      dataSet
    }

    override def prepareSession(): SparkSession = {
      val sparkConf = new SparkConf(loadDefaults = true)
        .set("nec.testing.target", testingTarget.label)
        .set("nec.testing.testing", this.toString)
        .set("spark.sql.codegen.comments", "true")
      testingTarget match {
        case TestingTarget.Rapids =>
          SparkSession
            .builder()
            .appName(name.value)
            .master("local[*]")
            .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
            .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
            .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")
            .config(key = "spark.ui.enabled", value = false)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.VectorEngine =>
          LocalVeoExtension._enabled = true
          SparkSession
            .builder()
            .master("local[*]")
            .appName(name.value)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
            .config(key = "spark.ui.enabled", value = false)
            .config(
              key = "spark.sql.columnVector.offheap.enabled",
              value = offHeapMode.get.offHeapEnabled.toString
            )
            .config(
              key = "spark.sql.inMemoryColumnarStorage.compressed",
              value = offHeapMode.get.compressed.toString()
            )
            .withExtensions(sse =>
              if (csvStrategy.isNative)
                sse.injectPlannerStrategy(sparkSession =>
                  NativeCsvStrategy(new LocalVeoExtension.LocalVeoNativeEvaluator(sparkSession.sparkContext.getConf))
                )
            )
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.PlainSpark =>
          SparkSession
            .builder()
            .master("local[*]")
            .appName(name.value)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
        case TestingTarget.CMake =>
          SparkSession
            .builder()
            .master("local[*]")
            .appName(name.value)
            .withExtensions(sse =>
              if (csvStrategy.isNative)
                sse.injectPlannerStrategy(sparkSession => NativeCsvStrategy(CNativeEvaluator))
            )
            .withExtensions(sse =>
              sse.injectPlannerStrategy(sparkSession =>
                VERewriteStrategy(sparkSession, CNativeEvaluator)
              )
            )
            .config(CODEGEN_FALLBACK.key, value = false)
            .config(CODEGEN_COMMENTS.key, value = true)
            .config(key = "spark.ui.enabled", value = false)
            .config(sparkConf)
            .getOrCreate()
      }
    }
  }

  val possibilities: List[Testing] =
    List(
      for {
        source <- List(SampleSource.CSV, SampleSource.Parquet)
        testingTarget <- List(
          TestingTarget.VectorEngine,
          TestingTarget.PlainSpark,
          TestingTarget.Rapids,
          TestingTarget.CMake
        )
        colMode <-
          if (testingTarget == TestingTarget.VectorEngine) VeColumnMode.All.map(v => Some(v))
          else List(None)
        csvStrat <-
          if (testingTarget.isNative && source == SampleSource.CSV) CsvStrategy.All
          else List(CsvStrategy.NormalCsv)
      } yield SimpleSql(
        sql = s"SELECT SUM(${SampleColA}), AVG(${SampleColB}) FROM nums",
        expectedResult = (62, 4),
        source = source,
        testingTarget = testingTarget,
        offHeapMode = colMode,
        csvStrategy = csvStrat
      ),
      JoinPlanSpec.OurTesting
    ).flatten

  trait BenchTestAdditions { this: AnyFreeSpec =>
    def runTestCase(testing: Testing): Unit = {
      testing.name.value in {
        val sparkSession = testing.prepareSession()
        val data = testing.prepareInput(sparkSession, DataSize.SanityCheckSize)
        try {
          testing.verifyResult(data.collect().toList)
        } catch {
          case e: Throwable =>
            throw new RuntimeException(s"${data.queryExecution.executedPlan}, ${e}", e)
        } finally testing.cleanUp(sparkSession)
      }
    }
  }

  val possibilitiesMap: Map[String, Testing] =
    possibilities.map(testing => testing.name.value -> testing).toMap
}

final class BenchTestingPossibilities extends AnyFreeSpec with BenchTestAdditions {

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)

}
