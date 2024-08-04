import scala.collection.mutable.ListBuffer

class Batch(private val S:Int) {
  private val batches: ListBuffer[Array[Double]] = ListBuffer.empty[Array[Double]]
  private var index: Int = 0

  def aggregate(element: Array[Double]): Unit = {
    if (index < S) {
      batches += element
      index += 1
    } else {
      throw new IllegalStateException("The batch is full")
    }
  }

  def isFull: Boolean = index == S

  def obtainBatch: ListBuffer[Array[Double]] = batches

  def obtainIndex: Int = index

  def copy(): Batch = {
    val copyBatch = new Batch(S)
    copyBatch.index = index
    for (i <- 0 until index) {
      copyBatch.batches += batches(i).clone()
    }
    copyBatch
  }

  def clean(): Unit = {
    batches.clear()
    index = 0
  }
}
