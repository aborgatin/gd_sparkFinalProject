package com.griddynamics.aborgatin.finalproject

object Model {

  case class Purchase(price: String, ip: Long)

  case class GeoBlock(ipMask: String, countryId: String)

  case class GeoLocation(countryId: String, countryName: String)

}
