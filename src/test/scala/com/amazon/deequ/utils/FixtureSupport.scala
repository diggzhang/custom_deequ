/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Date, Timestamp}
import scala.util.Random


trait FixtureSupport {

  def getEmptyColumnDataDf(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("", "a", "f"),
      ("", "b", "d"),
      ("", "a", null),
      ("", "a", "f"),
      ("", "b", null),
      ("", "a", "f")
    ).toDF("att1", "att2", "att3")
  }

  def getDfEmpty(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val column1 = $"column1".string
    val column2 = $"column2".string
    val mySchema = StructType(column1 :: column2 :: Nil)

    sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], mySchema)
  }

  def getDfWithNRows(sparkSession: SparkSession, n: Int): DataFrame = {
    import sparkSession.implicits._

    (1 to n)
      .toList
      .map { index => (s"$index", s"c1-r$index", s"c2-r$index")}
      .toDF("c0", "c1", "c2")
  }

  def getDfWithNestedColumn(sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

//    val schema = new StructType()
//      .add("dc_id", StringType)
//      .add("lang", StringType)
//      .add("source",
//        MapType(
//          StringType,
//          new StructType()
//            .add("description", StringType)
//            .add("weblink", StringType)
//            .add("ip", StringType)
//            .add("id", LongType)
//            .add("temp", LongType)
//            .add("c02_level", LongType)
//            .add("geo",
//              new StructType()
//                .add("lat", DoubleType)
//                .add("long", DoubleType)
//            )
//        )
//      )

    // Create a single entry with id and its complex and nested data types
    Seq("""
      {
      "dc_id": "dc-101",
      "lang":"en-us",
      "source": {
          "sensor-igauge": {
            "id": 10,
            "ip": "68.28.91.22",
            "description": "Sensor attached to the container ceilings",
            "weblink": "http://www.testing-url.dk/",
            "temp":35,
            "c02_level": 1475,
            "geo": {"lat":38.00, "long":97.00}
            }
          }
        }
      }""").toDF()
  }


  def getDfMissing(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "f"),
      ("2", "b", "d"),
      ("3", null, "f"),
      ("4", "a", null),
      ("5", "a", "f"),
      ("6", null, "d"),
      ("7", null, "d"),
      ("8", "b", null),
      ("9", "a", "f"),
      ("10", null, null),
      ("11", null, "f"),
      ("12", null, "d")
    ).toDF("item", "att1", "att2")
  }

  def getDfFull(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "c"),
      ("2", "a", "c"),
      ("3", "a", "c"),
      ("4", "b", "d")
    ).toDF("item", "att1", "att2")
  }

  def getDfWithNegativeNumbers(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "-1", "-1.0"),
      ("2", "-2", "-2.0"),
      ("3", "-3", "-3.0"),
      ("4", "-4", "-4.0")
    ).toDF("item", "att1", "att2")
  }

  def getDfCompleteAndInCompleteColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "f"),
      ("2", "b", "d"),
      ("3", "a", null),
      ("4", "a", "f"),
      ("5", "b", null),
      ("6", "a", "f")
    ).toDF("item", "att1", "att2")
  }

  def getDfDateTypeColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val requiredTimestampSchema = StructType(
      List(
        StructField("__uuid__", IntegerType, true),
        StructField("__timestamp__", TimestampType, true)
      )
    )
    val requiredDateTypeSchema = StructType(
      List(
        StructField("__uuid__", IntegerType, true),
        StructField("__timestamp__", DateType, true),
        StructField("__num__", StringType, true),
      )
    )

    val timestampDF = Seq(
      (1, new Timestamp(1646021692)),
      (2, new Timestamp(1646021692))
    ).toDF().rdd
    val dateTypeDF = Seq(
      (1, Date.valueOf("2021-08-05"), "6666"),
      (2, Date.valueOf("2021-08-06"), "8888")
    ).toDF().rdd

    val tsDf = Seq(
      (1, Timestamp.valueOf("2014-01-01 23:00:01")),
      (1, Timestamp.valueOf("2014-11-30 12:40:32")),
      (2, Timestamp.valueOf("2016-12-29 09:54:00")),
      (2, Timestamp.valueOf("2016-05-09 10:12:43"))
    ).toDF("typeId","__timestamp__")

    val dateTypeFrame = sparkSession.createDataFrame(dateTypeDF, requiredDateTypeSchema)
    val timestampTypeFrame = sparkSession.createDataFrame(timestampDF, requiredTimestampSchema)
    (timestampTypeFrame, dateTypeFrame)
//    dateTypeFrame
//    println("原始")
//    tsDf.show(false)
//    timestampTypeFrame.show(false)
//    dateTypeFrame.show(false)
//    println("转后")
//    tsDf.withColumn("sss", col("__timestamp__").cast(LongType)).show(false)
//    timestampTypeFrame.withColumn("__timestamp__", col("__timestamp__").cast(LongType))
//      .show(false)
//    timestampTypeFrame.withColumn("__timestamp__", col("__timestamp__").cast("date")).show(false)
//    timestampTypeFrame.withColumn("__timestamp__", col("__timestamp__").cast("long"))
//    timestampTypeFrame
//    tsDf.withColumn("sss", col("__timestamp__").cast(LongType))
    tsDf
//    dateTypeFrame.show(false)
//    dateTypeFrame
  }

  def getDfCompleteAndInCompleteColumnsDelta(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("7", "a", null),
      ("8", "b", "d"),
      ("9", "a", null)
    ).toDF("item", "att1", "att2")
  }


  def getDfFractionalIntegralTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1.0"),
      ("2", "1")
    ).toDF("item", "att1")
  }

  def getDfFractionalStringTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1.0"),
      ("2", "a")
    ).toDF("item", "att1")
  }

  def getDfIntegralStringTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1"),
      ("2", "a")
    ).toDF("item", "att1")
  }

  def getDfWithNumericValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    // att2 is always bigger than att1
    Seq(
      ("1", 1, 0, 0),
      ("2", 2, 0, 0),
      ("3", 3, 0, 0),
      ("4", 4, 5, 4),
      ("5", 5, 6, 6),
      ("6", 6, 7, 7)
    ).toDF("item", "att1", "att2", "att3")
  }

  def getDfWithNumericFractionalValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      ("1", 1.0, 0.0),
      ("2", 2.0, 0.0),
      ("3", 3.0, 0.0),
      ("4", 4.0, 5.0),
      ("5", 5.0, 6.0),
      ("6", 6.0, 7.0)
    ).toDF("item", "att1", "att2")
  }

  def getDfWithNumericFractionalValuesForKLL(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      ("1", 1.0, 0.0),
      ("2", 2.0, 0.0),
      ("3", 3.0, 0.0),
      ("4", 4.0, 5.0),
      ("5", 5.0, 6.0),
      ("6", 6.0, 7.0),
      ("7", 7.0, 0.0),
      ("8", 8.0, 0.0),
      ("9", 9.0, 0.0),
      ("10", 10.0, 5.0),
      ("11", 11.0, 6.0),
      ("12", 12.0, 7.0),
      ("13", 13.0, 0.0),
      ("14", 14.0, 0.0),
      ("15", 15.0, 0.0),
      ("16", 16.0, 5.0),
      ("17", 17.0, 6.0),
      ("18", 18.0, 7.0),
      ("19", 19.0, 0.0),
      ("20", 20.0, 0.0),
      ("21", 21.0, 0.0),
      ("22", 22.0, 5.0),
      ("23", 23.0, 6.0),
      ("24", 24.0, 7.0),
      ("25", 25.0, 0.0),
      ("26", 26.0, 0.0),
      ("27", 27.0, 0.0),
      ("28", 28.0, 5.0),
      ("29", 29.0, 6.0),
      ("30", 30.0, 7.0)
    ).toDF("item", "att1", "att2")
  }

  def getDfWithUniqueColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "0", "3", "1", "5", "0"),
      ("2", "0", "3", "2", "6", "0"),
      ("3", "0", "3", null, "7", "0"),
      ("4", "5", null, "3", "0", "4"),
      ("5", "6", null, "4", "0", "5"),
      ("6", "7", null, "5", "0", "6")
    )
    .toDF("unique", "nonUnique", "nonUniqueWithNulls", "uniqueWithNulls",
      "onlyUniqueWithOtherNonUnique", "halfUniqueCombinedWithNonUnique")
  }

  def getDfWithDistinctValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("a", null),
      ("a", null),
      (null, "x"),
      ("b", "x"),
      ("b", "x"),
      ("c", "y"))
      .toDF("att1", "att2")
  }

  def getDfWithConditionallyUninformativeColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 0),
      (2, 0),
      (3, 0)
    ).toDF("att1", "att2")
  }

  def getDfWithConditionallyInformativeColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 4),
      (2, 5),
      (3, 6)
    ).toDF("att1", "att2")
  }

  def getDfWithCategoricalColumn(
      sparkSession: SparkSession,
      numberOfRows: Int,
      categories: Seq[String])
    : DataFrame = {

    val random = new Random(0)

    import sparkSession.implicits._
    (1 to numberOfRows)
      .toList
      .map { index => (s"$index", random.shuffle(categories).head)}
      .toDF("att1", "categoricalColumn")
  }

  def getDfWithVariableStringLengthValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      "",
      "a",
      "bb",
      "ccc",
      "dddd"
    ).toDF("att1")
  }
}
