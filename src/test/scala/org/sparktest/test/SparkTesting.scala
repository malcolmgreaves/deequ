package sparktest.test

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Tag}

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import sparktest.util.sync.WaitGroup

object SparkTesting extends org.scalatest.Tag("spark") {

  // TURN OFF ALL OF THAT SPARK LOGGIN' !
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //
  //
  // Executing a function that requires use of the internal SparkContext.
  //
  //

  private[this] val wg = new WaitGroup

  def beforeSuite(): Unit =
    wg.add(1)

  def afterSuite(): Unit =
    wg.done(1)

  def awaitAllSparkTests(): Unit = {
    wg.waitUntilCompletion()
    shutdown()
  }

  def runWithSession(f: SparkSession => Any): Unit =
    runWithSession(f, defaultSparkConf)

  def runWithSession(f: SparkSession => Any,
                     configIfInit: => SparkConf): Unit = {

    // safely grab shared spark session
    val usingSparkSession = sesh(configIfInit)

    // execute the test
    wg.add(1)
    try {
      f(usingSparkSession)
      //Wwe don't catch exceptions as we want to propagate exceptional cases
      // to callers. We do, however, want to ensure that we properly decrement
      // from the internal wait group.
      ()

    } finally {
      wg.done(1)
    }
  }

  def runWithSpark(f: SparkContext => Any): Unit =
    runWithSpark(f, defaultSparkConf)

  def runWithSpark(f: SparkContext => Any, configIfInit: => SparkConf): Unit = {

    // safely grab shared spark context
    val usingSparkContext = sc(configIfInit)

    // execute the test
    wg.add(1)
    try {
      f(usingSparkContext)
      //Wwe don't catch exceptions as we want to propagate exceptional cases
      // to callers. We do, however, want to ensure that we properly decrement
      // from the internal wait group.
      ()

    } finally {
      wg.done(1)
    }
  }

  //
  //
  // Access to internal SparkContext.
  //
  //

  // we want fair locking so the constructor param is true
  private[this] val mutex = new ReentrantReadWriteLock(true)

  /**
    * WARNING :: Mutable! Global!
    *
    * Do not access this variable in any other manner except
    * through the `sc` and `shutdown` methods. This is to ensure that
    * there are no race conditions nor two spark contexts in the
    * same JVM at one time.
    */
  private[this] var scInternal: SparkContext = _

  /**
    * WARNING :: Mutable! Global!
    *
    * Do not access this variable in any other manner except
    * through the `sesh` and `shutdown` methods. This is to ensure that
    * there are no race conditions nor two spark contexts in the
    * same JVM at one time.
    */
  private[this] var seshInternal: SparkSession = _

  /**
    * A Spark configuration that is in local mode with 3 cores, uses the
    * Kryo serializer, and has the name "unit test XXX-XXXX" where the Xs
    * are random String elements.
    */
  lazy val defaultSparkConf: SparkConf =
    new SparkConf()
      .setMaster("local[3]")
      .setAppName(s"unit test ${Random.nextString(3)}-${Random.nextString(4)}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  /**
    * BLOCKING OPERATION
    *
    * Blocking must wait for interal write lock.
    */
  private[test] def sc(
    configIfInit: => SparkConf = defaultSparkConf
  ): SparkContext =
    try {
      mutex.writeLock.lock()
      if (scInternal == null) {
        scInternal = new SparkContext(configIfInit)
      }
      scInternal

    } finally {
      mutex.writeLock.unlock()
    }

  /**
    * BLOCKING OPERATION
    *
    * Blocking must wait for interal write lock.
    */
  private[test] def sesh(
    configIfInit: => SparkConf = defaultSparkConf
  ): SparkSession = {
    implicit val sparkContext: SparkContext = sc(configIfInit)
    try {
      mutex.writeLock.lock()
      if (seshInternal == null) {
        seshInternal = SparkSession.builder().config(configIfInit).getOrCreate()
      }
      seshInternal

    } finally {
      mutex.writeLock.unlock()
    }
  }

  /**
    * BLOCKING OPERATIONs
    *
    * Blocking must wait for internal write lock.
    */
  private[test] def shutdown(): Unit =
    try {
      mutex.writeLock.lock()

      if (seshInternal != null) {
        seshInternal.stop()
        seshInternal = null
      }

      if (scInternal != null) {
        scInternal.stop()
        scInternal = null
      }

      // To avoid Akka rebinding to the same port,
      // since it doesn't unbind immediately on shutdown.
      System.clearProperty("spark.master.port")
      System.gc()
      ()

    } finally {
      mutex.writeLock.unlock()
    }
}

trait SparkTesting extends FunSuite with BeforeAndAfterAll {

  final override def beforeAll(): Unit =
    SparkTesting.beforeSuite()

  final override def afterAll(): Unit = {
    SparkTesting.afterSuite()
    SparkTesting.awaitAllSparkTests()
  }

  /**
    * Calls the parent `test` method with the same parameters, except
    * that there is only one tag that is used: `AltSparkTesting`.
    */
  final def test(testName: String, ignored: Tag*)(testFun: => Unit): Unit =
    super.test(testName, SparkTesting)(testFun)

  /**
    * Calls the parent `ignore` method with the same parameters, except
    * that there is only one tag that is used: `AltSparkTesting`.
    */
  final def ignore(testName: String, ignored: Tag*)(testFun: => Unit): Unit =
    super.ignore(testName, SparkTesting)(testFun)

  /**
    * Runs as a FunSuite test: calls this overriden test method. Uses a shared
    * SparkContext to run the supplied test function `testFun`.
    */
  final def sparkTest(testName: String)(testFun: SparkContext => Any): Unit =
    test(testName) {
      SparkTesting.runWithSpark(testFun)
    }

  /**
    * An ignored Spark-enabled unit test. Useful when one must temporarily
    * disable a suite's test during gnarly debugging or development.
    *
    * NOTE:
    *
    * A stylistic break in naming convention is used in this method. Since its
    * use is in test code, this method name should never see the light of day
    * in any publicly facing APIs (or even internal ones). The concession in
    * naming convention is done out of the following practical observation:
    *
    * It is easier to prepend "ignore_" to an existing spark test definition than it
    * is to prepend "ignore", delete the "s", and then type in an "S" to keep
    * camel case naming convention. ("ignore_sparkTest" vs. "ignoreSparkTest")
    */
  final def ignore_sparkTest(
    testName: String
  )(ignoredTestFun: SparkContext => Any): Unit =
    ignore(testName) {
      ()
    }

  /**
    * Runs as a FunSuite test: calls this overriden test method. Uses a shared
    * SparkSession to run the supplied test function `testFun`.
    */
  final def sessionTest(testName: String)(testFun: SparkSession => Any): Unit =
    test(testName) {
      SparkTesting.runWithSession(testFun)
    }

  /**
    * An ignored Spark-enabled unit test. Useful when one must temporarily
    * disable a suite's test during gnarly debugging or development.
    *
    * NOTE:
    *
    * A stylistic break in naming convention is used in this method. Since its
    * use is in test code, this method name should never see the light of day
    * in any publicly facing APIs (or even internal ones). The concession in
    * naming convention is done out of the following practical observation:
    *
    * It is easier to prepend "ignore_" to an existing spark test definition than it
    * is to prepend "ignore", delete the "s", and then type in an "S" to keep
    * camel case naming convention. ("ignore_sessionTest" vs. "ignoreSessionTest")
    */
  final def ignore_sessionTest(
    testName: String
  )(ignoredTestFun: SparkSession => Any): Unit =
    ignore(testName) {
      ()
    }

}
