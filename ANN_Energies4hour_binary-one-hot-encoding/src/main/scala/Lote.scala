import scala.collection.mutable.ListBuffer

class Lote(private val S:Int) {
  private val lotes: ListBuffer[Array[Double]] = ListBuffer.empty[Array[Double]]
  private var index: Int = 0

  def agregar(elemento: Array[Double]): Unit = {
    if (index < S) {
      lotes += elemento
      index += 1
    } else {
      throw new IllegalStateException("El lote está lleno")
    }
  }

  def estaCompleto: Boolean = index == S

  def obtenerLote: ListBuffer[Array[Double]] = lotes

  def obtenerIndex: Int = index

  def copiar(): Lote = {
    val copiaLote = new Lote(S)
    copiaLote.index = index
    for (i <- 0 until index) {
      copiaLote.lotes += lotes(i).clone()
    }
    copiaLote
  }

  def clean(): Unit = {
    lotes.clear()
    index = 0
  }
}
