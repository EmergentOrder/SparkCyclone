package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.arrow.WithTestAllocator
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.{VeColVector, VeColVectorSource}
import org.bytedeco.veoffload.global.veo
import org.scalatest.freespec.AnyFreeSpec

final class SharedVeColVectorSpec extends AnyFreeSpec with WithVeProcess {
  "We can transfer from one VE process to another" in {
    val source: VeColVectorSource = VeColVectorSource(s"VE Tests")
    val proc1 = veo.veo_proc_create(0)
    val proc2 = veo.veo_proc_create(0)
    val veProcess1: VeProcess = VeProcess.WrappingVeo(proc1, source, VeProcessMetrics.NoOp)
    val veProcess2: VeProcess = VeProcess.WrappingVeo(proc2, source, VeProcessMetrics.NoOp)

    import OriginalCallingContext.Automatic._
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector =
          VeColVector.fromArrowVector(f8v)(veProcess1, source, implicitly[OriginalCallingContext])
        val sharedVec = colVec.toSharedMemory()(veProcess1)
        val secondVec = sharedVec.toLocalCol()(veProcess2, implicitly[OriginalCallingContext])
        val arrowVec = secondVec.toArrowVector()(veProcess2, alloc)
        try {
          colVec.free()(veProcess1, source, implicitly[OriginalCallingContext])
          expect(arrowVec.toString == f8v.toString)
        } finally arrowVec.close()
      }
    }
  }
}
