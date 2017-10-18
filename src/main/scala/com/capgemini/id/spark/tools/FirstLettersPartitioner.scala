package com.capgemini.id.spark.tools

import org.apache.spark.Partitioner

/**
  * A custom partitioner to distribute keys based on the first letter of a string
  * @param numParts
  */
class FirstLettersPartitioner(numParts: Int) extends Partitioner{
  override def numPartitions = numParts

  /**
    *
    * Give the partition number based on the string hash
    * @param key should be a string
    * @return
    */
  override def getPartition(key: Any) : Int = {
    val str = key.asInstanceOf[String]

    str.length compare 2 match {
      // longueur du mot de 2
      case 0 => nonNegativeMod(str.substring(2).hashCode, numParts)
      // longueur du mot de plus de 2 characters
      case 1 => nonNegativeMod(str.substring(3).hashCode,  numParts)
      // longueur du mot de moins de 2 characters
      case -1 => nonNegativeMod(str.substring(1).hashCode, numParts)
    }
  }

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}
