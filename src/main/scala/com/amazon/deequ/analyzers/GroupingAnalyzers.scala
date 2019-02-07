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

package com.amazon.deequ.analyzers

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{coalesce, col, count, expr, lit}
import Analyzers.COUNT_COL
import com.amazon.deequ.metrics.DoubleMetric
import Analyzers._
import org.apache.spark.sql.types.StructType
import Preconditions._
import com.amazon.deequ.schema.{ColumnName, SanitizeError}

/** Base class for all analyzers that operate the frequencies of groups in the data */
abstract class FrequencyBasedAnalyzer(columnsToGroupOn: Seq[String])
  extends GroupingAnalyzer[FrequenciesAndNumRows, DoubleMetric] {

  override def groupingColumns(): Seq[String] = { columnsToGroupOn }

  override def computeStateFrom(data: DataFrame): Option[FrequenciesAndNumRows] = {
    Some(FrequencyBasedAnalyzer.computeFrequencies(data, groupingColumns()))
  }

  /** We need at least one grouping column, and all specified columns must exist */
  override def preconditions: Seq[StructType => Unit] = {
    Seq(atLeastOne(columnsToGroupOn)) ++ columnsToGroupOn.map { hasColumn } ++ super.preconditions
  }
}

object FrequencyBasedAnalyzer {

  /** Compute the frequencies of groups in the data, essentially via a query like
    *
    * SELECT colA, colB, ..., COUNT(*)
    * FROM DATA
    * WHERE colA IS NOT NULL AND colB IS NOT NULL AND ...
    * GROUP BY colA, colB, ...
    */
  def computeFrequencies(
      data: DataFrame,
      groupingColumns: Seq[String],
      numRows: Option[Long] = None)
    : FrequenciesAndNumRows = {

    val columnsToGroupBy = groupingColumns.map { name => col(name) }.toArray
    val projectionColumns = columnsToGroupBy :+ col(COUNT_COL)

    val noGroupingColumnIsNull = groupingColumns
      .foldLeft(expr(true.toString)) { case (condition, name) =>
        condition.and(col(name).isNotNull)
      }

    val frequencies = data
      .select(columnsToGroupBy: _*)
      .where(noGroupingColumnIsNull)
      .groupBy(columnsToGroupBy: _*)
      .agg(count(lit(1)).alias(COUNT_COL))
      .select(projectionColumns: _*)

    val numRowsOfData = numRows match {
      case Some(count) => count
      case _ => data.count()
    }

    FrequenciesAndNumRows(frequencies, numRowsOfData)
  }
}

/** Base class for all analyzers that compute a (shareable) aggregation over the grouped data */
abstract class ScanShareableFrequencyBasedAnalyzer(name: String, columnsToGroupOn: Seq[String])
  extends FrequencyBasedAnalyzer(columnsToGroupOn) {

  def aggregationFunctions(numRows: Long): Seq[Column]

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): DoubleMetric = {

    state match {
      case Some(theState) =>
        val aggregations = aggregationFunctions(theState.numRows)

        val result = theState.frequencies.agg(aggregations.head, aggregations.tail: _*).collect()
          .head

        fromAggregationResult(result, 0)

      case None =>
        metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    }
  }

  override private[deequ] def toFailureMetric(exception: Exception): DoubleMetric = {
    metricFromFailure(exception, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  protected def toSuccessMetric(value: Double): DoubleMetric = {
    metricFromValue(value, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
  }

  def fromAggregationResult(result: Row, offset: Int): DoubleMetric = {
    if (result.isNullAt(offset)) {
      metricFromEmpty(this, name, columnsToGroupOn.mkString(","), entityFrom(columnsToGroupOn))
    } else {
      toSuccessMetric(result.getDouble(offset))
    }
  }

}

/** State representing frequencies of groups in the data, as well as overall #rows */
case class FrequenciesAndNumRows(frequencies: DataFrame, numRows: Long)
  extends State[FrequenciesAndNumRows] {

  /** Add up frequencies via an outer-join */
  override def sum(other: FrequenciesAndNumRows): FrequenciesAndNumRows = {

    val columns = frequencies.schema.fields
      .map { _.name }
      .filterNot { _ == COUNT_COL }

    val projectionAfterMerge = {
      val (sanitizedColumns, errors) = columns
        .map { ColumnName.sanitizeForSql }
        .foldLeft((Seq.empty[String], Seq.empty[SanitizeError])) {
          case ((sanitizedColumns, errors), sanitizeResult) => sanitizeResult match {
            case Right(c) => (sanitizedColumns :+ c, errors)
            case Left(e) => (sanitizedColumns, errors :+ e)
          }
        }
      if (errors.nonEmpty) {
        throw new IllegalArgumentException(
          s"Found ${errors.length} un-sanitizable column names:\n"+
            errors.mkString("\n")
        )
      } else {
        sanitizedColumns.map { c => coalesce(col(s"this.$c"), col(s"other.$c")).as(c) }
      }
    } ++
      Seq(
        (zeroIfNull(s"this.$COUNT_COL") + zeroIfNull(s"other.$COUNT_COL")).as(COUNT_COL)
      )

    /* Null-safe join condition over equality on grouping columns */
    val joinCondition = columns.tail
      .foldLeft(nullSafeEq(columns.head)) { case (expr, column) => expr.and(nullSafeEq(column)) }

    /* Null-safe outer join to merge histograms */
    val frequenciesSum = frequencies.alias("this")
      .join(other.frequencies.alias("other"), joinCondition, "outer")
      .select(projectionAfterMerge: _*)

    FrequenciesAndNumRows(frequenciesSum, numRows + other.numRows)
  }

  private[analyzers] def nullSafeEq(column: String): Column =
    ColumnName.sanitizeForSql(column) match {
      case Right(c) => col(s"this.$c") <=> col(s"other.$c")
      case Left(e) => throw e
    }

  private[analyzers] def zeroIfNull(column: String): Column =
    ColumnName.sanitizeForSql(column) match {
      case Right(c) => coalesce(col(c), lit(0))
      case Left(e) => throw e
    }

}


