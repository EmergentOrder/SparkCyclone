package com.nec.arrow.colvector

import org.apache.spark.sql.vectorized.ColumnarBatch

final case class ByteArrayColBatch(underlying: GenericColBatch[ByteArrayColVector]) {
  def toInternalColumnarBatch(): ColumnarBatch = {
    val cb = new ColumnarBatch(underlying.cols.map(_.toInternalVector()).toArray)
    cb.setNumRows(underlying.numRows)
    cb
  }
}
