package pcd.fsstatlib

import io.reactivex.rxjava3.subjects.CompletableSubject

import java.nio.file.Path
import java.util.concurrent.TimeUnit


private def printReport(report: Report): Unit =
  def formatBand(band: SizeBand): String = band match
    case SizeBand.Bounded(lower, upper) => s"[$lower, $upper]"
    case SizeBand.BoundedBelow(lower) => s"[$lower, ...]"
  IO.println(s"Reporting a total of ${report.fileCount} files:")
  for (band, count) <- report.sizeDistribution do
    IO.println(s"${formatBand(band)}: $count files.")
  IO.println("End of report.")


@main
def main(): Unit =
  val startingDirectory = Path.of("/") // automatically maps to something like C:\ on Windows
  IO.println(s"Scanning \"${startingDirectory.toAbsolutePath}\" for 5 seconds, reporting every 2 seconds...")
  val stopper = CompletableSubject.create()
  Thread(() =>
    Thread.sleep(5000)
    if !stopper.hasComplete then
      IO.println("Stopping early. Will now show the last report.")
      stopper.onComplete()
  ).start()
  FSStatLib.getFSReportInteractive(startingDirectory, 100_000, 10, stopper)
    .throttleLatest(2, TimeUnit.SECONDS, emitLast = true)
    .onBackpressureDrop()
    .blockingSubscribe(
      r => printReport(r),
      e => IO.println(s"Failed to access the starting directory: $e")
    )
