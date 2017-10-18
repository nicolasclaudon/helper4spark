package com.capgemini.id.spark.tools

import org.scalatest.FlatSpec

class FirstLettersPartitionerTest extends FlatSpec {


  "The partition number for two values" must "be different" in {
    val input0 = "experient"
    val input1 = "sc"
    val p = new FirstLettersPartitioner(2)
    val expectedInput0 = p.getPartition(input0)
    val expectedInput1 = p.getPartition(input1)
    assert(expectedInput0 != expectedInput1)
  }

  "The partition number" must "be positivie" in {
    val input0 = "experient"
    val input1 = "sc"
    val p = new FirstLettersPartitioner(2)
    val expectedInput0 = p.getPartition(input0)
    val expectedInput1 = p.getPartition(input1)
    val partitionIsPositive = (expectedInput0 >= 0) && (expectedInput1 >= 0)
    assert(partitionIsPositive)
  }
}
