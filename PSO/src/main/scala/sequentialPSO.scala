import scala.util.Random
class sequentialPSO(x: List[Array[Double]], y: List[Double], nInputs: Int, nHidden: Int, nIters: Int, nParts: Int, pos_max: Double, convergenceCurve_ : Array[Double]) {
  val FA = new AuxiliaryFunctions
  val nWeights: Int =nHidden*(nInputs + 1)
  // Number of weights dimensions
  val n = nWeights
  // Number of particles
  val m = nParts
  // Number of iterations
  val I = nIters
  var particles = Array.empty[Array[Double]]
  var best_global_pos = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  val rand = new Random
  val W = 1.0
  val c_1 = 0.8
  val c_2 = 0.2
  val V_max = 0.6*pos_max
  // Converting lists into serialisable arrays
  val xSer: Array[Array[Double]] = x.toArray
  val ySer: Array[Double] = y.toArray

  // Initialise vectors
  def init_weights(){
    for (i <- 0 until m) {
      //Here it may be better to initialise the last positions to smaller values
      val position = Array.fill(nWeights)(FA.Uniform(2, rand))
      val velocity = Array.fill(nWeights)(FA.Uniform(2, rand))
      val fit = FA.MSERed(xSer, ySer, position, nInputs, nHidden)
      val part_ = position ++ velocity ++ position ++ Array(fit)
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        best_global_pos = position
      }
      particles = particles :+ part_
    }
    //(best_global_fitness, mejor_pos_global, parts_)
  }

  def processing(): Unit = {
    for (i <- 0 until I) {
      for (j <- 0 until m) {
        var particle = particles(j)
        particle = FA.fitnessEval(xSer, ySer, particle, nInputs, nHidden)
        val fit = particle(3*nWeights)
        if (fit < best_global_fitness) {
          best_global_fitness = fit
          best_global_pos = particle.slice(0, nWeights)
        }
        particle = FA.posEval(particle, best_global_pos, nWeights, rand, W, c_1, c_2, V_max, pos_max)
        particles(j) = particle
      }
      convergenceCurve_(i)= best_global_fitness
    }
  }

  def get_weights(): Array[Double] = {
    best_global_pos
  }

}
