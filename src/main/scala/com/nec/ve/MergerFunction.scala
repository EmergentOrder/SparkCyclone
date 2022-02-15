package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.groupby.GroupByOutline

object MergerFunction {
  def mergeCVecStmt(vetype: VeType, index: Int): CodeLines = {
    val in = s"input_${index}_g"
    val out = s"output_${index}_g"
    val tmp = s"output_${index}"

    CodeLines.scoped(s"Merge ${in}[...] into ${out}[0]") {
      CodeLines.from(
        /*
          Allocate the nullable_T_vector[] with size buckets

          NOTE: This cast is incorrect, because we are allocating a T* array
          (T**) but type-casting it to T*.  However, for some reason, fixing
          this will lead an invalid free() later on.  Will need to investigate
          and fix this in the future.
        */
        s"// Allocate T*[] but cast to T* (incorrect but required to work correctly until a fix lands)",
        s"*${out} = static_cast<${vetype.cVectorType}*>(malloc(sizeof(nullptr)));",
        // Merge inputs and assign output to pointer
        s"${out}[0] = ${vetype.cVectorType}::merge(${in}, batches);",
      )
    }
  }

  def merge(types: List[VeType]): CFunction2 = {
    val inputs = types.zipWithIndex.map { case (veType, idx) =>
      CFunctionArgument.PointerPointer(veType.makeCVector(s"input_${idx}_g"))
    }

    val outputs = types.zipWithIndex.map { case (veType, idx) =>
      CFunctionArgument.PointerPointer(veType.makeCVector(s"output_${idx}_g"))
    }

    CFunction2(
      arguments = List(
        CFunctionArgument.Raw("int batches"),
        CFunctionArgument.Raw("int rows")
      ) ++ inputs ++ outputs,
      body = types.zipWithIndex.map((mergeCVecStmt _).tupled)
    )
  }
}
