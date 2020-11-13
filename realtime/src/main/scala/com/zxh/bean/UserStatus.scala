package com.zxh.bean

case class UserStatus(
    userId:String,  //用户id
    ifConsumed:String //是否消费过   0首单   1非首单
  )