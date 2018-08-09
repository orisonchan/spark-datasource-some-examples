package org.apache.spark.sql.sources.examples

import org.apache.spark.sql.sources

object FilterType extends Enumeration {
  type FilterType = Value
  val gt: sources.examples.FilterType.Value = Value("GreaterThan")
  val eq: sources.examples.FilterType.Value = Value("Equal")
  val lt: sources.examples.FilterType.Value = Value("LessThan")
  val and: sources.examples.FilterType.Value = Value("And")
  val or: sources.examples.FilterType.Value = Value("Or")
  val in: sources.examples.FilterType.Value = Value("in")
  val isNotNull: sources.examples.FilterType.Value = Value("isNotNull")
  val isNull: sources.examples.FilterType.Value = Value("isNull")
  val gte: sources.examples.FilterType.Value = Value("GreaterThanOrEqual")
  val lte: sources.examples.FilterType.Value = Value("LessThanOrEqual")
}
