class DAPSOController(dapso: DAPSO) {
  def recibirResultado(resultado: Array[Double], fitness: Double): Unit = {
    dapso.actualizarResultado(resultado, fitness)
  }
}
