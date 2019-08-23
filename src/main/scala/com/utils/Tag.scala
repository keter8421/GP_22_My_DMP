package com.utils

trait Tag {
  /**
    * 打标签的统一接口
    */
  def makeTags(arg:Any*):List[(String,Int)]
}
