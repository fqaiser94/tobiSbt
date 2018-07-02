package com.spark.transform

import com.spark.transform.HelloWorld._

class HelloWorldTest extends SparkTest {

  import spark.implicits._

  test("returns Hello World!") {
    main shouldEqual "Hello world!"
  }
}
