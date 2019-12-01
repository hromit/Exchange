package test_setup

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object TestSpark extends App{


  println("======== Test ==========")

  val spark = SparkSession.builder()
    .appName("SPhhhh")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  /*val someDF = Seq(
    (155973045020, "bat"),
    (155973045020, "mouse"),
    (155973045020, "horse")
  ).toDF("number", "word")*/


  val someData = Seq(
    Row(1572936419313L, "db1",1),
    Row(1572936419313L, "db1",2),
    Row(1572942241727L, "db1",1),
    Row(1572942241727L, "db1",3),
    Row(1572942241727L, "db2",1),
    Row(1572936419313L, "db2",1)
  )

  val someSchema = List(
    StructField("number", LongType, true),
    StructField("word", StringType, true),
    StructField("id", IntegerType, true)

  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )

  val PosTime = 1572936419313L

  // someDF.show(2)
  //  someDF.withColumn("number", ($"number" / 1000).cast(TimestampType)).show(false)

  //  someDF.printSchema()

  val res = getLatestRecs(someDF,List("id","word"),List("number"))
    .withColumn("number", ($"number" / 1000).cast(TimestampType))
    .withColumn("PAYMENT_AMOUNT",lit("50"))
    .withColumn("FEE_AMOUNT",lit("3.5"))
    .withColumn("SETTLEMENT_DATE",lit(current_timestamp()))
  // .foreach(row => println(row.get(0), row.get(1)))


  val t= res.select("number").rdd.map(r => r(0)).collect()

  print(t.length)

  // println(res)

  // res.show(false)


  /* someDF
     .where($"number" === someDF.groupBy().agg(max($"number")).map(_.getLong(0)).collect.head)
     .withColumn("number", ($"number" / 1000).cast(TimestampType))

     .show(false)*/




  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.functions._

  def getLatestRecs(df: DataFrame, partition_col: List[String], sortCols: List[String]): DataFrame = {
    val part = Window.partitionBy(partition_col.head,partition_col:_*).orderBy(array(sortCols.head,sortCols:_*).desc)
    val rowDF = df.withColumn("rn", row_number().over(part))
    val res = rowDF.filter("rn==1").drop("rn")
    res
  }


  // someDF.show(2)


  import collection.mutable.ListBuffer

  val list1 = ListBuffer[(String, String)]()
  list1 += (("Italy", "valid"))
  list1 += (("Germany", "not valid"))
  list1 += (("USA", "not valid"))
  list1 += (("Romania", "valid"))

  val list2 = ListBuffer[String]()
  list2 += "Germany"
  list2 += "USA"
  list2 += "Romania"
  list2 += "Italy"
  list2 += "France"
  list2 += "Croatia"


  val r = list1  map(_._1) intersect list2

  print(r)






}
