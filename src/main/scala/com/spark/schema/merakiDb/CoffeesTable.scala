package com.spark.schema.merakiDb

import slick.jdbc.PostgresProfile.api._
import slick.lifted.Tag

class CoffeesTable(tag: Tag) extends Table[CoffeesSchema](tag, "coffees") {
  def coffeeName = column[String]("coffeeName")
  def price = column[Double]("price")
  def * = (coffeeName, price).mapTo[CoffeesSchema]
}