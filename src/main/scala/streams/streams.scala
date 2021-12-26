package streams

import zio.*

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}

/**
 * Streams from amazing Zymposium sessions:
 * https://www.youtube.com/watch?v=y21EnJ28mpM
 * https://www.youtube.com/watch?v=T5vBs6_W_Xg
 *
 * Real zio-streams 1.x have declarative encoding tough
 */
object ZStreamTest extends ZIOAppDefault {

  val simpleSink = ZSink.runCollect[String]
  val simpleSink2 = ZSink.toFile("sink-zip-test")
  val notSoSimpleSink = simpleSink.zipWithPar(simpleSink2)((o, _) => o)
  val simpleStream = ZStream.fromIterator(List(4, 5, 6).iterator)

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] =
    for {
      // it is processing elements one by one
      // and it process only 3 elements even if take is the last stream operator unlike List[Int]
      res1 <- ZStream.fromIterator(List(1, 2, 3, 4, 5).iterator)
        .tap(a => ZIO.debug(s"WOW $a"))
        .map(_ + 100)
        .tap(a => ZIO.debug(s"BIGGER $a"))
        .take(3)
        .map(_.toString)
        .runCollect
      _ <- ZIO.debug(res1)

      //      _ <- ZStream.lines("build.sbt").tap(a => ZIO.debug(a).delay(500.milliseconds)).runDrain

      _ <- simpleStream.map(_.toString).run(notSoSimpleSink).debug("To Sink")
    } yield ExitCode.success
}

object Pull {
  def emit[A](a: A) = ZIO.succeed(a)

  def end = ZIO.fail(None)
}

// A internally will be Chunk[A] for performance reasons
case class ZStream[-R, +E, +A](
  process: ZManaged[R, E, ZIO[R, Option[E], A]]
) { self =>

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

  def run[R1 <: R, E1 >: E, O](sink: ZSink[R1, E1, A, O]): ZIO[R1, E1, O] =
    process.zip(sink.run).use { case (pull, push) =>
      def loop: ZIO[R1, E1, O] =
        pull.foldZIO(
          {
            case Some(e) => ZIO.fail(e)
            case None => push(Chunk.empty).flatMap {
              case Some(o) => ZIO.succeed(o)
              case None => ZIO.dieMessage("Sink violated contract by returning None after being pushed empty Chunk")
            }
          },
          a => push(Chunk.single(a)).flatMap {
            case Some(o) => ZIO.succeed(o)
            case None => loop
          }
        )

      loop
    }

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

  //FIXME
//  def runToFile(file: String)(implicit ev: A <:< String, ev2: E <:< Throwable): ZIO[R, Throwable, Unit] =
//  def runToFile(file: String)(implicit ev: A =:= String): ZIO[R, Throwable, Unit] =
//    self.run(ZSink.toFile(file))

  //  def runToFile(file: String)(implicit ev: A <:< String): ZIO[R, E, Unit] =
  //    process.use { pull =>
  //      ZIO.succeed(new File(file)).flatMap(
  //        file => ZIO.succeed(new BufferedWriter(new FileWriter(file))).flatMap {
  //          writer =>
  //            lazy val loop: ZIO[R, E, Unit] =
  //              pull.foldZIO(
  //                {
  //                  case None => ZIO.unit
  //                  case Some(err) => ZIO.fail(err)
  //                },
  //                a => ZIO.succeed(writer.write(a)) *> loop
  //              )
  //            loop.ensuring(ZIO.succeed(writer.close()))
  //        }
  //      )
  //    }
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

// PROTOCOL
// Empty Chunk of input means no more input
// Succeed with Some(O) means "I'm done with summary value O"
// Succeed with None means "Feed me more input"
// Fail with E means "I'm done with an error E"
// "Leftovers" can be added
case class ZSink[-R, +E, -I, +O](
  run: ZManaged[R, E, Chunk[I] => ZIO[R, E, Option[O]]]
) { self =>

  def zipWithPar[R1 <: R, E1 >: E, I1 <: I, O2, O3](
    that: ZSink[R1, E1, I1, O2]
  )(f: (O, O2) => O3): ZSink[R1, E1, I1, O3] =
    ZSink(
      self.run.zipPar(that.run).zipPar(Ref.make[Option[Either[O, O2]]](None).toManaged).map {
        case (pushLeft, pushRight, ref) =>
          in =>
            ref.get.flatMap {
              case None =>
                pushLeft(in).zipPar(pushRight(in)).flatMap {
                  case (Some(o), Some(o2)) => ZIO.some(f(o, o2))
                  case (Some(o), None)     => ref.set(Some(Left(o))).as(None)
                  case (None, Some(o2))    => ref.set(Some(Right(o2))).as(None)
                  case (None, None)        => ZIO.none
                }
              case Some(Left(o)) =>
                pushRight(in).map {
                  case Some(o2) => Some(f(o, o2))
                  case None     => None
                }
              case Some(Right(o2)) =>
                pushLeft(in).map {
                  case Some(o) => Some(f(o, o2))
                  case None     => None
                }
            }
      }
    )
}

object ZSink {

  def runCollect[A]: ZSink[Any, Nothing, A, Chunk[A]] =
    ZSink(
      Ref.make[Chunk[A]](Chunk.empty).toManaged.map(ref =>
        in =>
          // Decided for contract that empty chunk means end of stream
          if (in.isEmpty) ref.get.asSome
          else ref.update(_ ++ in).as(None)
      )
    )

  def writer(file: String) =
    ZManaged.acquireReleaseWith(
      ZIO.attempt(new BufferedWriter(new FileWriter(file)))
    )(writer => ZIO.attempt(writer.close()).ignore)

  def toFile(file: String): ZSink[Any, Throwable, String, Unit] =
    ZSink(writer(file).map { writer =>
      in =>
        if (in.isEmpty) ZIO.some(())
        else ZIO.foreachDiscard(in)(s => ZIO.succeed(writer.write(s))).as(None)
    })
}

