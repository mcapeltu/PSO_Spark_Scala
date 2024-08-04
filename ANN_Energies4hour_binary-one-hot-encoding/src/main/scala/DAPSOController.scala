class DAPSOController(dapso: DAPSO) {
  def receiveResult(resul: Array[Double], fitness: Double): Unit = {
    dapso.updateResult(resul, fitness)
  }
}
