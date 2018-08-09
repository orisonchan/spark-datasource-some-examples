package org.apache.spark.sql.sources.examples

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.examples.FilterType.FilterType
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class TPCDSDataSource extends SchemaRelationProvider {
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType
                             ): BaseRelation = TPCDSScan(schema, parameters)(sqlContext)
}

case class TPCDSScan(s: StructType, parameters: Map[String, String])(@transient val context: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with Logging {
  override def sqlContext: SQLContext = context

  override def schema: StructType = s

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val tableName = parameters.get("table")
    if (tableName.isEmpty)
      return null

    def getIndex(column: String): Int = {
      for (i <- schema.indices) {
        if (schema(i).name.equalsIgnoreCase(column))
          return i
      }
      -1
    }

    //    println(s">> table: ${tableName.get}")
    //    println(s">> schema: $schema" + schema)
    //    print(">> required: ")
    //    requiredColumns.foreach(x => print(x + ", "))
    //    println

    /** get rdd(string) start **/

    val sourceRDD = sqlContext.sparkContext.textFile(s"file:///Users/tpc-ds/v2.9.0rc2/datas/${tableName.get}.dat", 8)

    /** get rdd(string) end **/

    /** get rdd(row) start **/
    def mappingTransform(index: Int, element: String): Any = {
      val fieldType = schema(index)
      if (element.equals(""))
        return null
      fieldType.dataType match {
        case _: StringType => element
        case _: IntegerType => Integer.valueOf(element)
        case _: DateType => java.sql.Date.valueOf(element)
        case _: DecimalType => new java.math.BigDecimal(element)
      }
    }

    val rowRDD = sourceRDD.map(x => {
      val elements = x.split("\\|")
      var i = 0
      val seq = new ListBuffer[Any]
      while (i < schema.length) {
        if (i < elements.length)
          seq.append(mappingTransform(i, elements(i)))
        else
          seq.append(mappingTransform(i, ""))
        i = i + 1
      }
      Row.fromSeq(seq)
    })

    /** get rdd(row) end **/

    /** get filtered rdd(row) start **/
    def compare(left: Any, right: Any): Int = {
      left match {
        case i: Int => i.compareTo(right.asInstanceOf[Int])
        case f: Float => f.compareTo(right.asInstanceOf[Float])
        case _ => -2
      }
    }

    def oneMatching(filter: Filter): (Int, FilterType, Any) = filter match {
      case EqualTo(attribute, value) => (getIndex(attribute), FilterType.eq, value)
      case LessThan(attribute, value) => (getIndex(attribute), FilterType.lt, value)
      case LessThanOrEqual(attribute, value) => (getIndex(attribute), FilterType.lte, value)
      case GreaterThan(attribute, value) => (getIndex(attribute), FilterType.gt, value)
      case GreaterThanOrEqual(attribute, value) => (getIndex(attribute), FilterType.gte, value)
      case In(attribute, values) => (getIndex(attribute), FilterType.in, values)
      case IsNotNull(attribute) => (getIndex(attribute), FilterType.isNotNull, null)
      case IsNull(attribute) => (getIndex(attribute), FilterType.isNull, null)
      case And(left, right) => (-1, FilterType.and, Seq(oneMatching(left), oneMatching(right)))
      case Or(left, right) => (-1, FilterType.or, Seq(oneMatching(left), oneMatching(right)))
      case x => println(x.getClass.getName)
        (-1, null, null)
    }

    val filtersInfos = filters.map(oneMatching)

    def shouldExist(row: Row): Boolean = {
      filtersInfos.foreach(info => {
        def oneFilter(columnIndex: Int, filterType: FilterType, valueInFilter: Any): Boolean = {
          filterType match {
            case FilterType.eq =>
              if (row(columnIndex) == null)
                return false
              row(columnIndex).equals(valueInFilter)
            case FilterType.lt => compare(row(columnIndex), valueInFilter) < 0
            case FilterType.lte => compare(row(columnIndex), valueInFilter) <= 0
            case FilterType.gt => compare(row(columnIndex), valueInFilter) > 0
            case FilterType.gte => compare(row(columnIndex), valueInFilter) >= 0
            case FilterType.in => valueInFilter.asInstanceOf[Array[Any]].contains(row(columnIndex))
            case FilterType.isNotNull => row(columnIndex) != null
            case FilterType.isNull => row(columnIndex) == null
            case FilterType.and =>
              val seq = valueInFilter.asInstanceOf[Seq[(Int, FilterType, Any)]]
              oneFilter(seq.head._1, seq.head._2, seq.head._3) && oneFilter(seq(1)._1, seq(1)._2, seq(1)._3)
            case FilterType.or =>
              val seq = valueInFilter.asInstanceOf[Seq[(Int, FilterType, Any)]]
              oneFilter(seq.head._1, seq.head._2, seq.head._3) || oneFilter(seq(1)._1, seq(1)._2, seq(1)._3)
            case _ => true
          }
        }

        val flag = oneFilter(info._1, info._2, info._3)
        if (!flag)
          return false
      })
      true
    }

    val filteredRDD = rowRDD.filter(shouldExist)

    /** get filtered rdd(row) end **/

    /** get project rdd(row) start **/
    val columnsIndices = requiredColumns.map(getIndex)
    val projectRDD = filteredRDD.map(x => {
      var i = 0
      val seq = new ListBuffer[Any]
      while (i < schema.length) {
        if (columnsIndices.contains(i))
          seq.append(x(i))
        i = i + 1
      }
      Row.fromSeq(seq)
    })
    /** get project rdd(row) end **/

    val requiredSchema = columnsIndices.map(x => schema(x)).toSeq
    sqlContext.createDataFrame(projectRDD, StructType(requiredSchema)).rdd
  }
}
