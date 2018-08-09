package org.apache.spark.sql.sources.examples

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

class ScottDataSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val tableName = parameters("table")
    new ScottScan(tableName, schema)(sqlContext.sparkSession)
  }
}

object ScottScan {
  private def empData = Seq(
    Seq(7369, "Smith", 7902, 20, 800f),
    Seq(7902, "Ford", 7566, 20, 3000f),
    Seq(7566, "Jones", 7839, 20, 2975f),
    Seq(7499, "Allen", 7698, 30, 1600f),
    Seq(7521, "Ward", 7698, 30, 1250f),
    Seq(7698, "Blake", 7839, 30, 2950f),
    Seq(7839, "King", null, 10, 5000f)
  )

  private def deptData = Seq(
    Seq(10, "accounting", "New York"),
    Seq(20, "research", "Dallas"),
    Seq(30, "sales", "Chicago"),
    Seq(40, "operations", "Boston")
  )
}

case class ScottScan(tableName: String, s: StructType)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  override def schema: StructType = s

  override def sqlContext: SQLContext = sparkSession.sqlContext

  def data: Seq[Seq[Any]] = tableName match {
    case "dept" => ScottScan.deptData
    case "emp" => ScottScan.empData
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val data2Row = dataToRow(data, requiredColumns, filters)
    sqlContext.sparkContext.parallelize(data2Row.map(x => Row.fromSeq(x)))
  }

  private def dataToRow(data: Seq[Seq[Any]], requiredColumns: Array[String], filters: Array[Filter]): Seq[ListBuffer[Any]] = {
    val indexes = indices(requiredColumns)
    val filts = filters.map(x => getFilterAttributeIndexValue(x))
    val filteredData = data.filterNot(x => {
      shouldBeFilteredFunc(filts, filters, x)
    })
    filteredData.map(x => {
      val seq = new ListBuffer[Any]
      for (i <- indexes) {
        seq.append(x(i))
      }
      seq
    })
  }

  private def shouldBeFilteredFunc(filts: Array[Seq[(Int, Any, FilterType.FilterType)]], filters: Array[Filter], row: Seq[Any]): Boolean = {
    var shouldBeFiltered = false
    import util.control.Breaks._
    breakable {
      filts.foreach(filter => {
        if (filter.lengthCompare(3) == 0)
          filter(2)._3 match {
            case FilterType.and => shouldBeFiltered = shouldBeFilteredFunc(Array(Seq(filter.head)), filters, row) || shouldBeFilteredFunc(Array(Seq(filter(1))), filters, row)
            case FilterType.or => shouldBeFiltered = shouldBeFilteredFunc(Array(Seq(filter.head)), filters, row) && shouldBeFilteredFunc(Array(Seq(filter(1))), filters, row)
          }
        else {
          val realFilter = filter.head
          if (realFilter._3 != null)
            realFilter._3 match {
              case FilterType.gt => if (compare(row(realFilter._1), realFilter._2) <= 0) shouldBeFiltered = true
              case FilterType.eq => if (compare(row(realFilter._1), realFilter._2) != 0) shouldBeFiltered = true
              case FilterType.lt => if (compare(row(realFilter._1), realFilter._2) >= 0) shouldBeFiltered = true
            }
        }
        if (shouldBeFiltered)
          break()
      })
    }
    shouldBeFiltered
  }

  private def indices(requiredColumns: Array[String]): Array[Int] = {
    val result = new Array[Int](requiredColumns.length)
    for (i <- requiredColumns.indices)
      result(i) = getIndex(requiredColumns(i))
    result
  }

  private def getFilterAttributeIndexValue(filter: Filter): Seq[(Int, Any, FilterType.FilterType)] = {
    filter match {
      case GreaterThan(attribute, value) => Seq((getIndex(attribute), value, FilterType.gt))
      case LessThan(attribute, value) => Seq((getIndex(attribute), value, FilterType.lt))
      case EqualTo(attribute, value) => Seq((getIndex(attribute), value, FilterType.eq))
      case And(left, right) =>
        getFilterAttributeIndexValue(left) ++
          getFilterAttributeIndexValue(right) ++
          Seq((-1, None, FilterType.and))
      case Or(left, right) =>
        getFilterAttributeIndexValue(left) ++
          getFilterAttributeIndexValue(right) ++
          Seq((-1, None, FilterType.or))
      case _ => Seq((-1, None, null))
    }
  }

  private def getIndex(column: String): Int = {
    tableName match {
      case "emp" =>
        column match {
          case "empNo" => 0
          case "empName" => 1
          case "mgr" => 2
          case "deptNo" => 3
          case "salary" => 4
        }
      case "dept" =>
        column match {
          case "deptNo" => 0
          case "deptName" => 1
          case "loc" => 2
        }
    }
  }

  private def compare(left: Any, right: Any): Int = {
    left match {
      case i: Int => i.compareTo(right.asInstanceOf[Int])
      case f: Float => f.compareTo(right.asInstanceOf[Float])
      case _ => -2
    }
  }
}


