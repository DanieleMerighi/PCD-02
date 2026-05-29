package pcd.fsstatlib

import java.nio.file.Path


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
  FSStatLib.getFSReportInteractive(Path.of("."), 20, 4)
    .lastElement()
    .blockingSubscribe(
      r => printReport(r),
      e => IO.println(s"Failed to access the starting directory: $e")
    )

