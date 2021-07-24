package com.databeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructType}

object FlattenDataFrame {


  def flattenDataFrame(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case _: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataFrame(explodedDf)
        case structType: StructType =>
          val childFieldNames =
            structType.fieldNames.map(childname => fieldName + "." + childname)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
          import org.apache.spark.sql.functions.col

          val renamedCols =
            newFieldNames.map { x =>
              col(x.toString).as(x.toString.replace(".", "_"))
            }

          val explodedDf = df.select(renamedCols: _*)
          return flattenDataFrame(explodedDf)
        case _ =>
      }
    }
    df
  }
}



