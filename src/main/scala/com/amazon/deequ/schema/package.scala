package com.amazon.deequ
import com.softwaremill.tagging.@@

package object schema {

  /** Marker for Column type. */
  sealed abstract class C

  /** A sanitized column name: safe to use in a Spark SQL statement. */
  type SafeColumn = String @@ C

  /** Type constructor for Column: only way to make a Column instance. */
  @inline
  private[schema] def newColumn(s: String): SafeColumn =
    s.asInstanceOf[SafeColumn]

}
