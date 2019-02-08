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

package com.amazon.deequ.schema

import org.scalatest.{Matchers, WordSpec}

class ColumnNameTest extends WordSpec with Matchers {

  "ColumnName's sanitizeForSql function" should {

    "leave escaped column name unchanged" in {
      val c = "`escaped_column_name`"
      ColumnName.sanitizeForSql(c) match {
        case Right(x) => assert(c == x)
        case Left(x)  => fail(x.getMessage)
      }
    }

    "add leading ` if necessary" in {
      val c = "almost_escaped_column_name`"
      ColumnName.sanitizeForSql(c) match {
        case Right(x) => assert(x == s"`$c")
        case Left(x)  => fail(x.getMessage)
      }
    }

    "add trailing ` if necessary" in {
      val c = "`almost_escaped_column_name"
      ColumnName.sanitizeForSql(c) match {
        case Right(x) => assert(x == s"$c`")
        case Left(x)  => fail(x.getMessage)
      }
    }

    "surround column with `` when not escaped" in {
      val c = "]not escaped na[m]e[ "
      ColumnName.sanitizeForSql(c) match {
        case Right(x) => assert(x == s"`$c`")
        case Left(x)  => fail(x.getMessage)
      }
    }

    "fail to sanitize a column with a ` in the name" in {
      val c = "cannot_`_sanitize"
      ColumnName.sanitizeForSql(c) match {
        case Left(ColumnNameHasBackticks(column))  => assert(column == c)
        case x => fail(s"Expecting ColumnNameHasBackticks, not: $x")
      }
    }

    "fail to sanitize a null column name" in {
      ColumnName.sanitizeForSql(null) match {
        case Left(NullColumn)  => ()
        case x => fail(s"Expecting EmptyColumn, not: $x")
      }
    }
  }

}
