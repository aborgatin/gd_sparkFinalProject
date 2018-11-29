package com.griddynamics.aborgatin.finalproject

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object NetworkUtils {

  def getCountry(br: Broadcast[RDD[(Long, Long, String)]], ip: Long): String = {
    val range = br.value.collect()
    var left = 0
    var right = range.length
    var mid = 0
    while (!(left >= right)) {
      mid = left + (right - left) / 2
      if (ip >= range(mid)._1 && ip <= range(mid)._2)
        return range(mid)._3
      if (range(mid)._1 > ip)
        right = mid
      else
        left = mid + 1
    }
    val res = left - 1
    if (res > 0 && ip >= range(res)._1 && ip <= range(res)._2)
      range(res)._3
    else
      null
  }


  def maskToBound(mask: String, isLow: Boolean): Long = {
    val utils = new SubnetUtils(mask)
    val info = utils.getInfo
    if (isLow) {
      ipToNumber(info.getNetworkAddress)
    } else {
      ipToNumber(info.getBroadcastAddress)
    }
  }

  def ipToNumber(ip:String): Long = {
    val ipArr = ip.split("\\.")
    val res = new StringBuilder
    for (dec <- ipArr) {
      var decBinary = Integer.toBinaryString(dec.toInt)
      if (dec.length < 8) decBinary = new String(new Array[Char](8 - decBinary.length)).replace("\0", "0") + decBinary
      res.append(decBinary)
    }
    java.lang.Long.parseLong(res.toString, 2)
  }


}
