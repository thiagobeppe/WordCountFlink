package com.github.example

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

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
    val mapped : DataSet[(String,Int)] = filtered.map(new Tokenizer())

    if (params.has("output")) {
      filtered.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Scala WordCount Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      filtered.print()
    }
  }
//  class Tokenizer() extends MapFunction[String, (String,Int)] {
//    override def map(value: String): (String, Int) = {
//      new Tuple2[String, Int](value,1)
//    }
//  }
}
