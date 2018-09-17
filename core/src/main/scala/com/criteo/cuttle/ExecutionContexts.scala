package com.criteo.cuttle

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.Try

object ExecutionContexts {

  sealed trait WrappedExecutionContext {
    val underlying: ExecutionContext
  }

  sealed trait Metrics {
    protected var _threadPoolSize: Long = 0
    def threadPoolSize(): Long = _threadPoolSize
  }

  implicit def serverECToEC(ec: ServerExecutionContext): ExecutionContext = ec.underlying
  implicit def sideEffectECToEC(ec: SideEffectExecutionContext): ExecutionContext = ec.underlying
  implicit def implicitServerECToEC(implicit ec: ServerExecutionContext): ExecutionContext = ec.underlying
  implicit def implicitSideEffectECToEC(implicit ec: SideEffectExecutionContext): ExecutionContext = ec.underlying

  sealed trait ServerExecutionContext extends WrappedExecutionContext with Metrics

  // dedicated threadpool to start new executions and run user-defined side effects
  sealed trait SideEffectExecutionContext extends WrappedExecutionContext with Metrics

  object SideEffectExecutionContext {
    def wrap(wrapRunnable: Runnable => Runnable)(implicit executionContext: SideEffectExecutionContext): SideEffectExecutionContext = {
      new SideEffectExecutionContext {
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
    val ServerECThreadCount = Value("com.criteo.cuttle.ExecutionContexts.ServerExecutionContext.nThreads")
    val SideEffectECThreadCount = Value("com.criteo.cuttle.ExecutionContexts.SideEffectExecutionContext.nThreads")

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
    implicit val serverExecutionContext = new ServerExecutionContext {
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

    implicit val sideEffectExecutionContext = new SideEffectExecutionContext {
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
