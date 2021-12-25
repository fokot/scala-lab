package streams

import zio.*

import java.io.{BufferedReader, FileReader}

/**
 * Streams from amazing Zymposium sessions:
 * https://www.youtube.com/watch?v=y21EnJ28mpM
 * https://www.youtube.com/watch?v=T5vBs6_W_Xg
 *
 * Real zio-streams 1.x have declarative encoding tough
 */
object ZStreamTest extends ZIOAppDefault {

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] =
    for {
      // it is processing elements one by one
      // and it process only 3 elements even if take is the last stream operator unlike List[Int]
      res1 <- ZStream.fromIterator(List(1, 2, 3, 4, 5).iterator)
        .tap(a => ZIO.debug(s"WOW $a"))
        .map(_ + 100)
        .tap(a => ZIO.debug(s"BIGGER $a"))
        .take(3)
        .runCollect
      _ <- ZIO.debug(res1)

      _ <- ZStream.lines("build.sbt").tap(a => ZIO.debug(a).delay(500.milliseconds)).runDrain
    } yield ExitCode.success
}

object Pull {
  def emit[A](a: A) = ZIO.succeed(a)

  def end = ZIO.fail(None)
}

// A internally will be Chunk[A] for performance reasons
case class ZStream[-R, +E, +A](
  process: ZManaged[R, E, ZIO[R, Option[E], A]]
) {

  def tap[R1 <: R](f: A => URIO[R1, Any]): ZStream[R1, E, A] =
    ZStream(
      process.map(_.tap(f))
    )

  def map[B](f: A => B): ZStream[R, E, B] =
    ZStream(
      process.map(_.map(f))
    )

  def take(n: Int): ZStream[R, E, A] =
    ZStream(Ref.make(n).toManaged.zip(process).map { case (ref, pull) =>
      ref.updateAndGet(_ - 1).flatMap(in => if (in < 0) Pull.end else pull)
    })

  def take2(n: Int): ZStream[R, E, A] =
    ZStream(Ref.make(n).toManaged.flatMap(
      ref => process.map(
        pull => ref.updateAndGet(_ - 1).flatMap(in => if (in < 0) Pull.end else pull)
      )
    ))


  def runCollect: ZIO[R, E, Chunk[A]] =
    process.use { pull =>
      val builder = ChunkBuilder.make[A]()
      lazy val loop: ZIO[R, E, Chunk[A]] =
        pull.foldZIO(
          {
            case None => ZIO.succeed(builder.result()) // stream failed with an error
            case Some(err) => ZIO.fail(err) // stream end
          },
          a => UIO(builder += a) *> loop // stream emitted and element
        )
      loop
    }

  def runDrain: ZIO[R, E, Unit] =
    process.use { pull =>
      lazy val loop: ZIO[R, E, Unit] =
        pull.foldZIO(
          {
            case None => ZIO.unit // stream failed with an error
            case Some(err) => ZIO.fail(err) // stream end
          },
          a => loop // stream emitted and element
        )
      loop
    }
}

object ZStream {

  def fromIterator[A](iterator: => Iterator[A]): ZStream[Any, Nothing, A] =
    ZStream(
      ZManaged.succeed(iterator).map(
        it => // it.hasNext needs to be in effect, otherwise it will be stuck with the first value
          ZIO.succeed(it.hasNext).flatMap(b =>
            if (b)
              Pull.emit(it.next())
            else
              Pull.end
          )
      )
    )

  def lines(file: String): ZStream[Any, Throwable, String] =
    ZStream(
      ZManaged.acquireReleaseAttemptWith(new BufferedReader(new FileReader(file)))(_.close()).map(
        reader => ZIO.succeed(reader.ready()).flatMap(b =>
          if b then ZIO.attempt(reader.readLine()).asSomeError else Pull.end
        )
      )
    )
}



