package com.jaeyeong.datalake.datapipeline.lock

import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationZKDistLockTest extends FlatSpec with Matchers {

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def awaitBeforeTerminateMainThread(tSeconds:Int): Unit ={
    Await.result({
      Future{
        for(i <- 1 to tSeconds){
          println(i + " Seconds passed...")
          Thread.sleep(1000)
        }
      }
    }, tSeconds+5 second)
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  "Reader/Writer" should "acquire the Lock exclusively & Reader should read the correct ZNode data which is written by Writer." ignore  {

    val testRootNodeName = "/" + ZKDistLock.getZnAppRoot_0D + "/hbaccess_test"

    val testTargetLockNodeName = testRootNodeName + "/rw-lock"
    val testTargetSharedDataNode = testRootNodeName + "/read/active/tbl-name"

    val f_reader = {
      Future{
        while(true) {

          Thread.sleep(3000)    // read per 3sec

          println("[Reader #1]. Try to acquire Lock")
          val lockedNode = ZKDistLock.acquireLock(testTargetLockNodeName)
          println("[Reader #2]. Lock acquired")

          // Critical Section - STARTED
          val res = ZKDistLock.getData(testTargetSharedDataNode)
          println("[Reader #3 - Critical Section] : " + res.getOrElse(""))
          //Thread.sleep(1000)
          // Critical Section - END

          println("[Reader #4]. Try to release Lock")
          ZKDistLock.releaseLock(lockedNode)
          println("[Reader #5]. Lock released")
        }
      }
    }

    val f_writer = {
      Future{
        while(true) {

          Thread.sleep(10000)    // writer per 10sec

          println("   [Writer #1]. Try to acquire Lock")
          val lockedNode = ZKDistLock.acquireLock(testTargetLockNodeName)
          println("   [Writer #2]. Lock acquired")

          // Critical Section - STARTED
          val now = Calendar.getInstance().getTime()
          val minuteFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
          val currentMinuteAsString = minuteFormat.format(now)

          // Critical Section - STARTED
          ZKDistLock.setData(testTargetSharedDataNode, currentMinuteAsString)
          println("   [Writer #3 - Critical Section] : " + testTargetSharedDataNode + " -> " + currentMinuteAsString)
          //Thread.sleep(1000)
          // Critical Section - END

          println("   [Writer #4]. Try to release Lock")
          ZKDistLock.releaseLock(lockedNode)
          println("   [Writer #5]. Lock released")
        }
      }
    }

    Await.result(f_reader, Duration.Inf)
    Await.result(f_writer, Duration.Inf)

    awaitBeforeTerminateMainThread(60)

    true
  }
}
