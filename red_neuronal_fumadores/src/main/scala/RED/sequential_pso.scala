package RED

import scala.util.Random

class sequential_pso(x: List[Array[Double]], y: List[Double], nInputs: Int, nHidden: Int, nIters: Int, nParts: Int, pos_max: Double, c1: Double, c2: Double) {
  val FA = new Auxiliary_Functions
  val nWeights: Int =nHidden*(nInputs + 1)
  //If we use the 2 layer network
  //val nWeights: Int =nHidden*(nInputs + nHidden + 1)
  // Weights dimension number
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
  val c_1 = c1
  val c_2 = c2
  val V_max = 0.1*pos_max
  // Convert lists into serializable arrays
  val xSer: Array[Array[Double]] = x.toArray
  val ySer: Array[Double] = y.toArray

  // We inilitialize vectors
  def initialize_weights(){
    for (i <- 0 until m) {
      //Here it may be better to initialise the last positions to smaller values.
      val position = Array.fill(nWeights)(FA.Uniform(2, rand))
      val velocity = Array.fill(nWeights)(FA.Uniform(2, rand))
      val fit = FA.binaryEntropyRed(xSer, ySer, position, nInputs, nHidden)
      val part_ = position ++ velocity ++ position ++ Array(fit)
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        best_global_pos = position
        //println("fit " + fit)
      }
      particles = particles :+ part_
    }
    //(best_global_fitness, best_global_pos, parts_)
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
    }
  }
  def get_weights(): Array[Double] = {
    best_global_pos
  }
}
