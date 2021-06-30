package org.rubigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

object RegExEx {
  def main(args: Array[String]) {
    val warcfile = s"hdfs:///single-warc-segment"
    //val warcfile = s"hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-0000?.warc.gz"
    
    val sparkConf = new SparkConf()
                          .setAppName("RegExEx")
			  .set("spark.memory.offHeap.enabled", "true")
			  .set("spark.memory.offHeap.size", "8g")
                          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                          .registerKryoClasses(Array(classOf[WarcRecord]))
    implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    val warcs = sc.newAPIHadoopFile(
                  warcfile,
                  classOf[WarcGzInputFormat],             // InputFormat
                  classOf[NullWritable],                  // Key
                  classOf[WarcWritable]                   // Value
        )

    val matches = warcs.
                map { wr => wr._2}.
                filter{ _.isValid() }.
                map{ _.getRecord() }.
                filter{ _.getHeader.getHeaderValue("WARC-Type") == "response" }.
                filter{ _.getHttpMimeType() == "text/html" }.
                map{ _.getHttpStringBody() }.
                map{ text => ("(?i)italy".r.findAllIn(text).size,  "(?i)austria".r.findAllIn(text).size ) }

    val nums = matches.reduce({case((x1, p1), (x2, p2)) => (x1 + x2, p1 + p2)})
    println("Count of italy: \t\t" + nums._1)
    println("Count of austria: \t" + nums._2)
    
    sparkSession.stop()
  }
}
