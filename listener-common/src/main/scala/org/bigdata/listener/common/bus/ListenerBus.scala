package org.bigdata.listener.common.bus

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, CopyOnWriteArrayList, LinkedBlockingDeque}

import org.bigdata.listener.common.events.Event
import org.bigdata.listener.common.listeners.ListenerInterface
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal


/**
 *  强烈建议子类是单例模式
 *
 * Author: GL
 * Date: 2022-12-16
 */
abstract class ListenerBus[L <: ListenerInterface, E <: Event](name: String) {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  // 监听器集合
  private[this] val listeners = new CopyOnWriteArrayList[L]
  // 事件集合
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()
  private[this] val started = new AtomicBoolean(false)
  // 消费线程
  private[this] val dispatchThread = new Thread(s"listenerBus") {
    setDaemon(true)
    override def run(): Unit = {
      dispatch()
    }
  }

  def post(event: E): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"ListenerBus stoped! event:$event")
    }
    eventQueue.put(event)
  }

  private[this] final def dispatch(): Unit = {
    try {
      while(started.get()) {
        val e = eventQueue.take()
        if (e != null) {
          postToAll(e)
        }
      }
    } catch {
      case _: InterruptedException => // 线程被中断退出
      case NonFatal(e) => logger.error("Unexpected error in " + name, e)
    }
  }

  private[this] final def postToAll(event: E): Unit = {
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          try {
            onError(e)
          } catch {
            case NonFatal(e) => logger.error("Unexpected error in " + name, e)
          }
      }
    }
  }

  final def start(name: Option[String] = None): Unit = {
    if (started.compareAndSet(false, true)) {
      dispatchThread.start()
    } else {
      throw new IllegalStateException(s"${name.getOrElse("listenersBus")} already started!")
    }
  }

  final def stop(name: Option[String] = None): Unit = {
    if(!started.compareAndSet(true, false)) {
      throw new IllegalStateException(s"Attempted to stop ${name.getOrElse("listenersBus")} that has not yet started!")
    }
  }

  final def getQueueSize: Int = {
    eventQueue.size()
  }

  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  final def removeListener(listener: L): Unit = {
    listeners.remove(listener)
  }

  final def removeAllListeners(): Unit = {
    listeners.clear()
  }

  // 各子类消息总线实现该模版函数
  protected def doPostEvent(listener: L, event: E): Unit

  protected def onError(e: Throwable): Unit

}
