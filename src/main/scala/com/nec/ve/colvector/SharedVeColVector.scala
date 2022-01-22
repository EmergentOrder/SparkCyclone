package com.nec.ve.colvector

import com.nec.arrow.colvector.GenericColVector
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext

final case class SharedVeColVector(underlying: GenericColVector[Long]) {
  def toLocalCol()(implicit veProcess: VeProcess, originalCallingContext: OriginalCallingContext): VeColVector = ???
}
