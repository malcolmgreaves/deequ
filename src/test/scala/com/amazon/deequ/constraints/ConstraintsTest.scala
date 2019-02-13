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

package com.amazon.deequ
package constraints

import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}
import ConstraintUtils.calculate
import com.amazon.deequ.analyzers.{Completeness, NumMatchesAndCount}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType}
import Constraint._
import com.amazon.deequ.SparkContextSpec

class ConstraintsTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Completeness constraint" should {
    "assert on wrong completeness" in withSparkSession { sparkSession =>

      def test(df: DataFrame, col1: String, col2: String): Unit = {
        assert(calculate(Constraint.completenessConstraint(col1, _ == 0.5), df).status ==
          ConstraintStatus.Success)
        assert(calculate(Constraint.completenessConstraint(col1, _ != 0.5), df).status ==
          ConstraintStatus.Failure)
        assert(calculate(Constraint.completenessConstraint(col2, _ == 0.75), df).status ==
          ConstraintStatus.Success)
        assert(calculate(Constraint.completenessConstraint(col2, _ != 0.75), df).status ==
          ConstraintStatus.Failure)
      }

      val df = getDfMissing(sparkSession)
      test(df, "att1", "att2")
      test(specialNamesDfAtt1Att2(df), ")att1(", "]att2[")
    }
  }

  "Histogram constraints" should {
    "assert on bin number" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        assert(calculate(Constraint.histogramBinConstraint(column, _ == 3), df).status ==
          ConstraintStatus.Success)
        assert(calculate(Constraint.histogramBinConstraint(column, _ != 3), df).status ==
          ConstraintStatus.Failure)
      }

      val df = getDfMissing(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on ratios for a column value which does not exist" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
          val metric = calculate(Constraint.histogramConstraint(column,
            _("non-existent-column-value").ratio == 3), df)

          metric match {
            case result =>
            assert(result.status == ConstraintStatus.Failure)
            assert(result.message.isDefined)
            assert(result.message.get.startsWith(AnalysisBasedConstraint.AssertionException))
          }
        }

      val df = getDfMissing(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }
  }

  "Mutual information constraint" should {
    "yield a mutual information of 0 for conditionally uninformative columns" in
      withSparkSession { sparkSession =>

        def test(df: DataFrame, col1: String, col2: String): Unit = {
          calculate(Constraint.mutualInformationConstraint(col1, col2, _ == 0), df)
            .status shouldBe ConstraintStatus.Success
        }

        val df = getDfWithConditionallyUninformativeColumns(sparkSession)
        test(df, "att1", "att2")
        test(specialNamesDfAtt1Att2(df), ")att1(", "]att2[")
      }
  }

  "Basic stats constraints" should {
    "assert on approximate quantile" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.approxQuantileConstraint(column, quantile = 0.5, _ == 3.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on minimum" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.minConstraint(column, _ == 1.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on maximum" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.maxConstraint(column, _ == 6.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on mean" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.meanConstraint(column, _ == 3.5), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on sum" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.sumConstraint(column, _ == 21), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on standard deviation" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.standardDeviationConstraint(column, _ == 1.707825127659933), df)
         .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }

    "assert on approximate count distinct" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(Constraint.approxCountDistinctConstraint(column, _ == 6.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithNumericValues(sparkSession)
      test(df, "att1")
      test(specialNamesDfAtt1Att2(df), ")att1(")
    }
  }

  "Correlation constraint" should {
    "assert maximal correlation" in withSparkSession { sparkSession =>

      def test(df: DataFrame, col1: String, col2: String): Unit = {
        calculate(Constraint.correlationConstraint(col1, col2, _ == 1.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      test(df, "att1", "att2")
      test(specialNamesDfAtt1Att2(df), ")att1(", "]att2[")
    }
    "assert no correlation" in withSparkSession { sparkSession =>

      def test(df: DataFrame, col1: String, col2: String): Unit = {
        calculate(Constraint.correlationConstraint(col1, col2, java.lang.Double.isNaN), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = getDfWithConditionallyUninformativeColumns(sparkSession)
      test(df, "att1", "att2")
      test(specialNamesDfAtt1Att2(df), ")att1(", "]att2[")
    }
  }

  "Data type constraint" should {
    val column = "column"

    "assert fractional type for DoubleType column" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(dataTypeConstraint(column, ConstrainableDataTypes.Fractional, _ == 1.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = dataFrameWithColumn(column, DoubleType, sparkSession, Row(1.0), Row(2.0))
      test(df, column)
      test(df.withColumnRenamed(column, s"]$column["), s"]$column[")
    }

    "assert fractional type for StringType column" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(dataTypeConstraint(column, ConstrainableDataTypes.Fractional, _ == 0.5), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = dataFrameWithColumn(column, StringType, sparkSession, Row("1"), Row("2.0"))
      test(df, column)
      test(df.withColumnRenamed(column, s"]$column["), s"]$column[")
    }

    "assert numeric type as sum over fractional and integral" in withSparkSession { sparkSession =>

      def test(df: DataFrame, column: String): Unit = {
        calculate(dataTypeConstraint(column, ConstrainableDataTypes.Numeric, _ == 1.0), df)
          .status shouldBe ConstraintStatus.Success
      }

      val df = dataFrameWithColumn(column, StringType, sparkSession, Row("1"), Row("2.0"))
      test(df, column)
      test(df.withColumnRenamed(column, s"]$column["), s"]$column[")
    }
  }

  "Anomaly constraint" should {
    "assert on anomaly analyzer values" in withSparkSession { sparkSession =>

      def test(df: DataFrame, col1: String, col2: String): Unit = {
        assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
          Completeness(col1), _ > 0.4), df).status == ConstraintStatus.Success)
        assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
          Completeness(col1), _ < 0.4), df).status == ConstraintStatus.Failure)

        assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
          Completeness(col2), _ > 0.7), df).status == ConstraintStatus.Success)
        assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
          Completeness(col2), _ < 0.7), df).status == ConstraintStatus.Failure)
      }

      val df = getDfMissing(sparkSession)
      test(df, "att1", "att2")
      test(specialNamesDfAtt1Att2(df), ")att1(", "]att2[")
    }
  }
}
