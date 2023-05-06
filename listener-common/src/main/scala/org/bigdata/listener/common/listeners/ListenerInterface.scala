package org.bigdata.listener.common.listeners

import org.bigdata.listener.common.events.Event

/**
 * Author: GL
 * Date: 2022-12-16
 */
trait ListenerInterface {
  def onOtherEvent(e: Event)
}
