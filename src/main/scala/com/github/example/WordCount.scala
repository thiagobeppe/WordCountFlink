package com.github.example

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //Get the environment of the job
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //Take the parameters from CLI
    val params: ParameterTool = ParameterTool.fromArgs(args)
    //Make this parameters available in all cluster nodes
    env.getConfig.setGlobalJobParameters(params)

    val textToCount: DataSet[String] = env.readTextFile("wc.txt")
    val filtered: DataSet[String] = textToCount.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = {
        value.startsWith("N")
      }
    })

    val tokenized: DataSet[(String, Int)] = filtered.map(new Tokenizer())

    tokenized.print()
  }

  class Tokenizer() extends MapFunction[String, (String, Int)] {
    override def map(value: String): (String, Int) = {
        (value, 1)
    }
  }
}

