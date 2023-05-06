package org.bigdata.listener.common.util

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

/**
 * 摘自spark的EventLoop[事件池]
 * 同样强烈建议子类是单例模式，避免多次创建增加系统负担
 *
 * @param name 事件池别名
 * @tparam E 事件类型
 */
abstract class EventLoop[E](name: String) {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  // 队列默认大小为: Integer.MAX_VALUE
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  private val stopped = new AtomicBoolean(false)

  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get && judge) {
          val event = eventQueue.take()
          try {
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logger.error("Unexpected error in " + name, e)
              }
          }
        }
      } catch {
        case ie: InterruptedException => stopped.set(true)
        case NonFatal(e) => logger.error("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  def post(event: E): Unit = {
    if (!stopped.get) {
      if (eventThread.isAlive) {
        eventQueue.put(event)
      } else {
        onError(new IllegalStateException(s"$name has already been stopped accidentally."))
      }
    }
  }

  def isActive: Boolean = eventThread.isAlive

  // 子类可以根据业务重写该类
  def judge: Boolean = true

  /**
   * Invoked when `start()` is called but before the event thread starts.
   */
  protected def onStart(): Unit = {}

  /**
   * Invoked when `stop()` is called and the event thread exits.
   */
  protected def onStop(): Unit = {}


  protected def onReceive(event: E): Unit

  protected def onError(e: Throwable): Unit

}
