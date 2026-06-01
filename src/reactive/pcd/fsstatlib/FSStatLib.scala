package pcd.fsstatlib

import io.reactivex.rxjava3.flowables.ConnectableFlowable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.CompletableSubject

import java.nio.file.Path


case class Report(fileCount: Int, sizeDistribution: Seq[(SizeBand, Int)])


object FSStatLib:

  def getFSReportInteractive(startingDirectory: Path, upperSize: Long, boundedBandCount: Int, stopper: Option[CompletableSubject] = None): ConnectableFlowable[Report] =
    val files = FileTreeFlowable(startingDirectory, stopper)
    val sizes = files
      .observeOn(Schedulers.computation())
      .map: (path, attributes) =>
        attributes.size()
    given BandsSpec = BandsSpec(upperSize, boundedBandCount)
    val reports = sizes
      .scan(emptyReport, addToReport)
      .map(prettifyResult)
    reports
      .replay(1)

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

