import java.io.PrintStream
import java.net._
import java.util.concurrent.{ExecutorService, Executors}
import java.io.{FileNotFoundException, IOException}

import scala.io.Source

import scala.io.BufferedSource

class Server extends Runnable {

  val server = new ServerSocket(9999)
  val pool: ExecutorService = Executors.newFixedThreadPool(5)

  def run(): Unit = {
    while (true) {
      val s = server.accept()
      pool.execute(new ServerController(s))
    }
  }
}

class ServerController(socket: Socket) extends Runnable {
  def run(): Unit = {
    val in = new BufferedSource(socket.getInputStream()).getLines()
    val out = new PrintStream(socket.getOutputStream())

    val filename = "/Users/mozilla/IdeaProjects/HelloWorld/build"
    try {
      for (line <- Source.fromFile(filename).getLines()) {
        out.print(line)
      }
    } catch {
      case ex: FileNotFoundException => println("Couldn't find that file.")
      case ex: IOException => println("Had an IOException trying to read that file")
    }

    out.flush()
    socket.close()
  }
}
