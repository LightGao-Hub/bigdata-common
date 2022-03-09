package com.haizhi.bigdata.common.asyn.enums

/**
 * Author: GL
 * Date: 2022-02-08
 */
sealed abstract class ViewType(val path: String, val code: Int) {
  override def toString: String = s"$path($code)"
}

object ViewType {
  def apply(code: Int): ViewType = {
    code match {
      case 0 => viewCreate
      case _ => viewCreate
    }
  }
}

case object viewCreate extends ViewType("/view/create", 0)



