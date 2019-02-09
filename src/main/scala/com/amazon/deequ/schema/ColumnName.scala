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

  /** A sanitization result: either an error or a sanitized `Column` instance. */
  type Sanitized = Either[SanitizeError, SafeColumn]

  /**
    * Sanitizes the input column name by ensuring that it is escaped with backticks.
    *
    * The resulting String is the escaped input column name, which is safe to use in
    * any Spark SQL statement.
    */
  @inline
  def sanitizeForSql(columnName: String): Sanitized =
    if (columnName == null) {
      Left(NullColumn)

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
        Right(newColumn(s"$prefix$columnName$suffix"))
      }
    }

  /**
    * Obtains the `String` value if `Right` or throws the `SanitizeError` if `Left`.
    **/
  @inline
  def getOrThrow(x: Sanitized): SafeColumn = x match {
    case Left(er) => throw er
    case Right(c) => c
  }

  /**
    * Obtains the `String` pair if both are `Right`, otherwise throws the error(s).
    *
    * If only one of the sanitizations failed, then a `SanitizeError` type is thrown.
    * If both fail, then an `IllegalArgumentException` is thrown and its message contains
    * both of the `SanitizeError` messages.
    * */
  @inline
  def getOrThrow(x: (Sanitized, Sanitized)): (SafeColumn, SafeColumn) = x match {
    case (Right(cA), Right(cB)) => (cA, cB)
    case (Left(eA), Left(eB)) =>
      throw new IllegalArgumentException(
        s"Cannot sanitize two column names:\n$eA\n$eB"
      )
    case (Left(e), _) => throw e
    case (_, Left(e)) => throw e
  }

  /**
    * Alias for `sanitizeForSql | getOrThrow`.
    *
    * @throws SanitizeError iff the column name cannot be sanitized.
    */
  @inline
  def sanitize(columnName: String): SafeColumn =
    getOrThrow(sanitizeForSql(columnName))

}

sealed abstract class SanitizeError(message: String) extends Exception(message)
case class ColumnNameHasBackticks(column: SafeColumn)
    extends SanitizeError(
      s"Column name ($column) has backticks (non-sanitizing), which is not allowed in Spark SQL."
    )
case object NullColumn
    extends SanitizeError("null is not a valid column name value")
