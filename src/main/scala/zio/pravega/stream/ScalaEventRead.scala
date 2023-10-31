package zio.pravega.stream

import io.pravega.client.stream.EventRead
import io.pravega.client.stream.Position
import io.pravega.client.stream.EventPointer

final class ScalaEventRead[A](event: A, eventRead: EventRead[Array[Byte]]) extends EventRead[A] {
  def getEvent(): A                   = event
  def getEventPointer(): EventPointer = eventRead.getEventPointer()
  def getPosition(): Position         = eventRead.getPosition()
  def getCheckpointName(): String     = eventRead.getCheckpointName()
  def isCheckpoint(): Boolean         = eventRead.isCheckpoint()
  def isReadCompleted(): Boolean      = eventRead.isReadCompleted()
}
