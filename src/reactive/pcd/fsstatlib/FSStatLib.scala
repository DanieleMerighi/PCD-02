package pcd.fsstatlib

import io.reactivex.rxjava3.observables.ConnectableObservable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.CompletableSubject

import java.nio.file.Path


case class Report(fileCount: Int, sizeDistribution: Seq[(SizeBand, Int)]):

  def formatToString: String =
    def formatBand(band: SizeBand): String = band match
      case SizeBand.Bounded(lower, upper) => s"[$lower, $upper]"
      case SizeBand.BoundedBelow(lower) => s"[$lower, ...]"
    val bandLines = sizeDistribution
      .map: (band, count) =>
        s"${formatBand(band)}: $count files."
      .mkString("\n")
    s"Reporting a total of $fileCount files:\n$bandLines"


object FSStatLib:

  def getFSReportInteractive(startingDirectory: Path,
                             upperSize: Long,
                             boundedBandCount: Int,
                             stopper: Option[CompletableSubject] = None): ConnectableObservable[Report] =
    val files = FileTreeObservable(startingDirectory)
    val sizes = files
      .observeOn(Schedulers.computation())
      .map: (path, attributes) =>
        attributes.size()
    given BandsSpec = BandsSpec(upperSize, boundedBandCount)
    var reports = sizes
      .scan(emptyReport, addToReport)
      .map(prettifyResult)
    if stopper.nonEmpty then
      reports = reports
        .takeUntil(stopper.get.toObservable)
        .doOnTerminate(() => stopper.get.onComplete())
    reports
      .publish()

  private case class InternalReport(fileCount: Int, sizeDistribution: Seq[Int])

  private def emptyReport(using bandsSpec: BandsSpec): InternalReport =
    InternalReport(0, Seq.fill(bandsSpec.totalBandCount)(0))

  private def addToReport(report: InternalReport, size: Long)(using bandsSpec: BandsSpec): InternalReport =
    val i = bandsSpec.findBandIndex(size)
    InternalReport(
      report.fileCount + 1,
      report.sizeDistribution.mapAt(i, _ + 1)
    )

  private def prettifyResult(report: InternalReport)(using bandsSpec: BandsSpec): Report =
    Report(report.fileCount, bandsSpec.makeBands.zip(report.sizeDistribution))

