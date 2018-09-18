package com.criteo.cuttle

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.Try

object ThreadPools {

  sealed trait WrappedThreadPool {
    val underlying: ExecutionContext
  }

  sealed trait Metrics {
    protected var _threadPoolSize: Long = 0
    def threadPoolSize(): Long = _threadPoolSize
  }

  implicit def serverECToEC(ec: ServerThreadPool): ExecutionContext = ec.underlying
  implicit def sideEffectECToEC(ec: SideEffectThreadPool): ExecutionContext = ec.underlying
  implicit def implicitServerECToEC(implicit ec: ServerThreadPool): ExecutionContext = ec.underlying
  implicit def implicitSideEffectECToEC(implicit ec: SideEffectThreadPool): ExecutionContext = ec.underlying

  sealed trait ServerThreadPool extends WrappedThreadPool with Metrics

  // dedicated threadpool to start new executions and run user-defined side effects
  sealed trait SideEffectThreadPool extends WrappedThreadPool with Metrics

  object SideEffectThreadPool {
    def wrap(wrapRunnable: Runnable => Runnable)(implicit executionContext: SideEffectThreadPool): SideEffectThreadPool = {
      new SideEffectThreadPool {
        private val delegate = executionContext.underlying

        override val underlying: ExecutionContext = new ExecutionContext {
          override def execute(runnable: Runnable): Unit = delegate.execute(wrapRunnable(runnable))
          override def reportFailure(cause: Throwable): Unit = delegate.reportFailure(cause)
        }

      }
    }
  }

  // The implicitly provided execution contexts use fixed thread pools.
  // These thread pool default sizes are overridable with Java system properties, passing -D<property_name> <value> flags when you start the JVM
  object ThreadPoolSystemProperties extends Enumeration {
    type ThreadPoolSystemProperties = Value
    val ServerECThreadCount = Value("com.criteo.cuttle.ThreadPools.ServerThreadPool.nThreads")
    val SideEffectECThreadCount = Value("com.criteo.cuttle.ThreadPools.SideEffectThreadPool.nThreads")

    def fromSystemProperties(value: ThreadPoolSystemProperties.Value, defaultValue: Int): Int =
      loadSystemPropertyAsInt(value.toString, defaultValue)

    private def loadSystemPropertyAsInt(propertyName: String, defaultValue: Int) = {
      Option(System.getProperties().getProperty(propertyName)) match {
        case Some(size) => Try[Int] { size.toInt }.getOrElse(Runtime.getRuntime.availableProcessors)
        case None => Runtime.getRuntime.availableProcessors
      }
    }
  }

  object Implicits {
    import ThreadPoolSystemProperties._
    implicit val serverThreadPool = new ServerThreadPool {
      override val underlying = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(
        fromSystemProperties(ServerECThreadCount, Runtime.getRuntime.availableProcessors),
        utils.createThreadFactory(
          (r: Runnable) => {
            val t = Executors.defaultThreadFactory.newThread(r)
            _threadPoolSize += 1
            t.setDaemon(true)
            t.setPriority(Thread.MAX_PRIORITY)
            t
          })
      ))
    }

    implicit val sideEffectThreadPool = new SideEffectThreadPool {
      override val underlying = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(
        fromSystemProperties(SideEffectECThreadCount, Runtime.getRuntime.availableProcessors),
        utils.createThreadFactory(
          (r: Runnable) => {
            val t = Executors.defaultThreadFactory.newThread(r)
            _threadPoolSize += 1
            t.setDaemon(true)
            t
          })
      ))
    }
  }
}
