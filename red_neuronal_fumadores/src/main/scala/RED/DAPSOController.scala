package RED

class DAPSOController(dapso: DAPSO) {
  def receiveResult(result: Array[Double], fitness: Double): Unit = {
    dapso.updateResult(result, fitness)
  }
}


