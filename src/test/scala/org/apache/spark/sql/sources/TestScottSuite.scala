package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.test.SharedSQLContext

class TestScottSuite  extends DataSourceTest with SharedSQLContext with PredicateHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(
      """
        |CREATE table if not exists emp(
        |empNo int,
        |empName string,
        |mgr int,
        |deptNo int,
        |salary float
        |)
        |USING org.apache.spark.sql.sources.examples.ScottDataSource
        |OPTIONS ("table" 'emp')
      """.stripMargin)
    sql(
      """
        |CREATE table if not exists dept(
        |deptNo int,
        |deptName string,
        |loc string
        |)
        |USING org.apache.spark.sql.sources.examples.ScottDataSource
        |OPTIONS ("table" 'dept')
      """.stripMargin)
  }

  test("SELECT * FROM dept") {
    val df = spark.sql("SELECT * FROM dept")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

  test("SELECT * FROM emp") {
    val df = spark.sql("SELECT * FROM emp")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

  test("SELECT empName FROM emp") {
    val df = spark.sql("SELECT empName FROM emp")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

  test("SELECT empName FROM emp WHERE empNo < 7600") {
    val df = spark.sql("SELECT empName FROM emp WHERE empNo < 7600")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

  test("SELECT deptNo, SUM(salary) from emp GROUP BY deptNo") {
    val df = spark.sql("select deptNo, SUM(salary) from emp GROUP BY deptNo")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

  test("SELECT COUNT(empNo), deptNo from emp GROUP BY deptNo") {
    val df = spark.sql("select COUNT(empNo), deptNo from emp GROUP BY deptNo")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

  test("select deptNo, AVG(salary) from emp GROUP BY deptNo") {
    val df = spark.sql("select deptNo, AVG(salary) from emp GROUP BY deptNo")
    println(df.queryExecution.optimizedPlan)
    df.show(false)
  }

}
