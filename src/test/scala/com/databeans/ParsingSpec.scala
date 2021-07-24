package com.databeans

import com.databeans.Parsing.flattenDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ParsingSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Parser_Test")
    .getOrCreate()

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

  }


}