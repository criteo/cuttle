package com.criteo.cuttle

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, ThreadFactory}

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.Try

object ThreadPools {

  sealed trait WrappedThreadPool {
    val underlying: ExecutionContext
  }

  sealed trait Metrics {
    def threadPoolSize(): Int
  }

  implicit def serverECToEC(ec: ServerThreadPool): ExecutionContext = ec.underlying
  implicit def sideEffectECToEC(ec: SideEffectThreadPool): ExecutionContext = ec.underlying
  implicit def implicitServerECToEC(implicit ec: ServerThreadPool): ExecutionContext = ec.underlying
  implicit def implicitSideEffectECToEC(implicit ec: SideEffectThreadPool): ExecutionContext = ec.underlying

  sealed trait ServerThreadPool extends WrappedThreadPool with Metrics

  // dedicated threadpool to start new executions and run user-defined side effects
  sealed trait SideEffectThreadPool extends WrappedThreadPool with Metrics

  object SideEffectThreadPool {
    def wrap(wrapRunnable: Runnable => Runnable)(
      implicit executionContext: SideEffectThreadPool): SideEffectThreadPool =
      new SideEffectThreadPool {
        private val delegate = executionContext.underlying

        override val underlying: ExecutionContext = new ExecutionContext {
          override def execute(runnable: Runnable): Unit = delegate.execute(wrapRunnable(runnable))
          override def reportFailure(cause: Throwable): Unit = delegate.reportFailure(cause)
        }

        override def threadPoolSize(): Int = executionContext.threadPoolSize()
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

    private def loadSystemPropertyAsInt(propertyName: String, defaultValue: Int) =
      Option(System.getProperties().getProperty(propertyName)) match {
        case Some(size) => Try[Int] { size.toInt }.getOrElse(Runtime.getRuntime.availableProcessors)
        case None       => Runtime.getRuntime.availableProcessors
      }
  }

  def newThreadFactory(daemonThreads: Boolean = true,
                       poolName: Option[String] = None,
                       threadCounter: AtomicInteger = new AtomicInteger(0)): ThreadFactory = new ThreadFactory() {
    override def newThread(r: Runnable): Thread = {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setDaemon(daemonThreads)
      poolName match {
        case Some(name) =>
          val threadCount = threadCounter.incrementAndGet()
          t.setName(s"$name-$threadCount")
        case None =>
      }
      t
    }
  }

  /**
    * @param daemonThreads set to true to create daemon threads (threads that do not prevent the JVM from exiting when the program finishes but the threads are still running)
    * @param poolName optional parameter to identify the threads created by this thread pool
    * @param threadCounter reference to a counter keeping track of the total number of threads created by this thread pool
    */
  def newFixedThreadPool(numThreads: Int,
                         daemonThreads: Boolean = true,
                         poolName: Option[String] = None,
                         threadCounter: AtomicInteger = new AtomicInteger(0)): ExecutorService =
    Executors.newFixedThreadPool(numThreads, newThreadFactory(daemonThreads, poolName, threadCounter))

  /**
    * @param daemonThreads set to true to create daemon threads (threads that do not prevent the JVM from exiting when the program finishes but the threads are still running)
    * @param poolName optional parameter to identify the threads created by this thread pool
    * @param threadCounter reference to a counter keeping track of the total number of threads created by this thread pool
    */
  def newScheduledThreadPool(numThreads: Int,
                             daemonThreads: Boolean = true,
                             poolName: Option[String] = None,
                             threadCounter: AtomicInteger = new AtomicInteger(0)): ScheduledExecutorService =
    Executors.newScheduledThreadPool(numThreads, newThreadFactory(daemonThreads, poolName, threadCounter))

  object Implicits {
    import ThreadPoolSystemProperties._
    implicit val serverThreadPool = new ServerThreadPool {
      private val _threadPoolSize: AtomicInteger = new AtomicInteger(0)

      override val underlying = ExecutionContext.fromExecutorService(
        newFixedThreadPool(fromSystemProperties(ServerECThreadCount, Runtime.getRuntime.availableProcessors),
                           poolName = Some("Server"),
                           threadCounter = _threadPoolSize)
      )

      override def threadPoolSize(): Int = _threadPoolSize.get()
    }

    implicit val sideEffectThreadPool = new SideEffectThreadPool {
      private val _threadPoolSize: AtomicInteger = new AtomicInteger(0)

      override val underlying = ExecutionContext.fromExecutorService(
        newFixedThreadPool(fromSystemProperties(SideEffectECThreadCount, Runtime.getRuntime.availableProcessors),
                           poolName = Some("SideEffect"),
                           threadCounter = _threadPoolSize)
      )

      override def threadPoolSize(): Int = _threadPoolSize.get()
    }
  }
}
