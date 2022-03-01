package com.amazon.deequ.analyzers.catalyst

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


class DateTimeAggregation(frequency: Long) extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", TimestampType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("map",
    DataTypes.createMapType(LongType, LongType)) :: Nil)

  override def dataType: DataType = DataTypes.createMapType(LongType, LongType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val map = Map.empty[Long, Long]
    buffer.update(0, map)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val datetime = input.getTimestamp(0).getTime
      val batchTime = datetime - (datetime % frequency)
      val bufferMap = buffer(0).asInstanceOf[Map[Long, Long]]
      buffer(0) = bufferMap + (batchTime -> (bufferMap.getOrElse(batchTime, 0L) + 1L))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val bufferMap1 = buffer1(0).asInstanceOf[Map[Long, Long]]
    val bufferMap2 = buffer2(0).asInstanceOf[Map[Long, Long]]
    buffer1(0) = bufferMap1 ++ bufferMap2.map {
      case (k, v) => k -> (v + bufferMap1.getOrElse(k, 0L))
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getMap(0)
  }
}
