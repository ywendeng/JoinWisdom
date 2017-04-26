package cn.jw.rms.ab.pred.shortterm

import breeze.linalg._
import breeze.numerics._


object BreezeTest {
  def main(args: Array[String]): Unit = {

    val y = DenseVector(-1.0,3.0,2.0)
    val X = DenseMatrix.zeros[Double](3,3)
    X(0,::) := DenseVector(2.0,1.0,1.0).t
    X(1,::) := DenseVector(-2.0,4.0,1.0).t
    X(2,::) := DenseVector(4.0,1.0,1.0).t

    println("y:")
    println(y)

    println("X:")
    println(X)

    println("X.t:")
    println(X.t)

    val step1 = X.t * X
    println("X.t * X:")
    println(step1)

    val step2 = inv(X.t * X)
    println("inv(X.t * X):")
    println(step2)

    val step3 = inv(X.t * X) * X.t
    println("inv(X.t * X) * X.t:")
    println(step3)

    val step4 = inv(X.t * X) * X.t * y
    println("inv(X.t * X) * X.t * y:")
    println(step4)

    //val result = inv(X.t * X) * X.t * y
    //println(result)

/*
    val m = DenseMatrix.zeros[Int](3,3)

    m(0,::) := DenseVector(1,2,3,4,5).t
    println(m)

    m(::,0) := DenseVector(6,7,8)
    println(m)*/


  }
}
