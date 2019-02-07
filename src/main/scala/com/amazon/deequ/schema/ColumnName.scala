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

object ColumnName {

  sealed abstract class SanitizeError(message: String) extends Exception(message)
  case class ColumnNameHasBackticks(column: String) extends SanitizeError(
    s"Column name ($column) has backticks (non-sanitizing), which is not allowed in Spark SQL."
  )
  case object EmptyColumn extends SanitizeError("Empty column name is invalid")

  /**
    * Sanitizes the input column name by ensuring that it is escaped with backticks.
    *
    * The resulting String is the escaped input column name, which is safe to use in
    * any Spark SQL statement.
    */
  def sanitizeForSql(columnName: String): Either[SanitizeError, String] =
    if (columnName == null || columnName.isEmpty || columnName.trim.isEmpty) {
      Left(EmptyColumn)

    } else {
      val (prefix, suffix, insideColumnName) = {
        val prefix = if (!columnName.startsWith("`")) "`" else ""
        val suffix = if (!columnName.endsWith("`")) "`" else ""
        val inside1 = if (prefix.isEmpty) {
          columnName.slice(1, columnName.length)
        } else {
          columnName
        }
        val inside2 = if (suffix.isEmpty) {
          inside1.slice(0, inside1.length - 1)
        } else {
          inside1
        }
        (prefix, suffix, inside2)
      }

      if (insideColumnName.contains("`")) {
        Left(ColumnNameHasBackticks(columnName))
      } else {
        Right(s"$prefix$columnName$suffix")
      }
    }

}
