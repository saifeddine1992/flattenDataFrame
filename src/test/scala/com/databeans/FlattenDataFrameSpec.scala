package com.databeans

import com.databeans.FlattenDataFrame.flattenDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


case class Data ( id: Long , data_created_at : Long , data_text : String , names_id : Long , names_hay_zouhour : String)

class FlattenDataFrameSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("flattenDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "flattenDataFrame" should "Flatten the json DataFrame" in {

    Given("The Json Dataframe  ")
    val df = spark.read.option("multiLine", true).json("sample.json")
    When("flattenDataFrame is invoked")
    val parsedFile = flattenDataFrame(df)
    val schemaReturned = parsedFile.schema
    Then("the Flattened DataFrame should be returned")
    val expectedResult = spark.read.option("multiLine", true).json("ParsedSample.json")
    val expectedSchema = expectedResult.schema
    expectedSchema should contain theSameElementsAs( schemaReturned )
    expectedResult.as[Data].collect() should contain theSameElementsAs(parsedFile.as[Data].collect())

  }
}