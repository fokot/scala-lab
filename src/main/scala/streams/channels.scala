package streams

import zio.*

import scala.annotation.tailrec

object ZChannelsTest extends ZIOAppDefault {
  import channels._

  val stream = ZStream.fromIterator(Iterator(1, 2, 3, 4, 5)).map(_ * 2)

  val run =
    stream.run(ZSink.runCollect).debug
//    (stream.channel >>> ZSink.runCollect.channel).runDrain.debug
}

object channels {

  object old {
    final case class ZStream[-Env, +OutErr, +OutElm, +OutDone](
      pull: ZManaged[Env, OutErr, ZIO[Env, OutErr, Either[Chunk[OutElm], OutDone]]]
    )
    final case class ZSink[-Env, +OutErr, -InElm, +OutDone](
      push: ZManaged[Env, OutErr, Chunk[InElm] => ZIO[Env, OutErr, Option[OutDone]]]
    )
  }


  // Input type = Requirement
  // Output type = Expectation
  // Requirements have a minimum expectation, the more you know the more you expect of a type and less you can accept (-)
  // Expectation get maximally vague, the more you return, the less you know but the more you can accept (+)

  // pull from upstream => to the downstream
  // With this encoding you a not only have Streams and Sinks but also encoders/decoders,
  // or stream which will output every element it pulls twice
  final case class ZChannel[-Env, -InErr, -InElem, -InDone, +OutErr, +OutElem, +OutDone](
    run: Managed[InErr, IO[InErr, Either[InElem, InDone]]] =>
      ZManaged[Env, OutErr, ZIO[Env, OutErr, Either[OutElem, OutDone]]]
  ) { self =>

    // compose
    def >>>[Env1 <: Env, OutErr2, OutElem2, OutDone2](
      that: ZChannel[Env1, OutErr, OutElem, OutDone, OutErr2, OutElem2, OutDone2]
    ): ZChannel[Env1, InErr, InElem, InDone, OutErr2, OutElem2, OutDone2] =
      ZChannel { upstream =>
// without environment, it is quite elegant
//          that.run(self.run(upstream))
        ZManaged.environment[Env].flatMap(environment =>
          that.run(self.run(upstream).map(_.provideEnvironment(environment)).provideEnvironment(environment))
        )
      }

    def mapElem[OutElem2](f: OutElem => OutElem2): ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem2, OutDone] =
      ZChannel {
        upstream =>
          run(upstream).map(_.map(_.left.map(f)))
      }

    def runDrain(implicit ev1: Any <:< InErr, ev2: Any <:< InElem, ev3: Any <:< InDone): ZIO[Env, OutErr, (Chunk[OutElem], OutDone)] =
      run(ZManaged.succeed(ZIO.right(()))).use { pull =>
        def loop(acc: Chunk[OutElem]): ZIO[Env, OutErr, (Chunk[OutElem], OutDone)] =
          pull.flatMap {
            case Left(elem) => loop(elem +: acc)
            case Right(done) => ZIO.succeed((acc, done))
          }
        loop(Chunk.empty)
      }

  }

  object ZChannel {

    def fromIteratorChunk[A](iterator: Iterator[A], chunkSize: Int): ZChannel[Any, Any, Any, Any, Nothing, Chunk[A], Any] =
      ZChannel(_ =>
        ZManaged.succeed(iterator.grouped(chunkSize)).map { iterator =>
          ZIO.succeed(iterator.hasNext).flatMap(b =>
            if(b)
              ZIO.left(Chunk.fromIterable(iterator.next()))
            else
              ZIO.right(())
          )
        }
      )
  }

  final case class ZStream[-R, +E, +A](
    channel: ZChannel[R, Any, Any, Any, E, Chunk[A], Any]
  ) { self =>

    def map[B](f: A => B): ZStream[R, E, B] =
      ZStream(channel.mapElem(_.map(f)))

    def run[R1 <: R, E2, Z](sink: ZSink[R1, E, E2, A, Z]): ZIO[R1, E2, Z] =
      (self.channel >>> sink.channel).runDrain.map(_._2)

  }

  object ZStream {

    def fromIterator[A](iterator: Iterator[A], chunkSize: Int = 1): ZStream[Any, Nothing, A] =
      ZStream(ZChannel.fromIteratorChunk(iterator, chunkSize))
  }

  final case class ZSink[-R, -EIn, +EOut, -I, +O](
    channel: ZChannel[R, EIn, Chunk[I], Any, EOut, Nothing, O]
  )

  object ZSink {
    def runCollect[E, A]: ZSink[Any, E, E, A, Chunk[A]] =
      ZSink(
        ZChannel { upstream =>
          upstream.map { pull =>
            def loop(acc: Chunk[A]): IO[E, Either[Nothing, Chunk[A]]] =
              pull.flatMap {
                case Left(chunk) => loop(acc ++ chunk)
                case Right(_) => ZIO.right(acc)
              }
            loop(Chunk.empty)
          }
        }
      )
  }

}
