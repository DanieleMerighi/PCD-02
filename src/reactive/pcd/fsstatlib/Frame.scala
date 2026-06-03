package pcd.fsstatlib

import io.reactivex.rxjava3.core.{BackpressureStrategy, Observable}
import io.reactivex.rxjava3.subjects.{CompletableSubject, PublishSubject}

import java.awt.{Component, Dimension}
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import javax.swing.*


class ReactiveFileChooser(showObservable: Observable[Unit],
                          pathSubject: PublishSubject[Path],
                          parent: Component) extends JFileChooser:

  private val dialog = createDialog(parent)
  setDialogType(JFileChooser.OPEN_DIALOG)
  dialog.setModal(true)

  showObservable.subscribe(_ => dialog.setVisible(true))

  private val commandSubject = PublishSubject.create[String]()
  addActionListener(e => commandSubject.onNext(e.getActionCommand))

  commandSubject.subscribe(_ => dialog.setVisible(false))

  commandSubject
    .filter(JFileChooser.APPROVE_SELECTION == _)
    .map(_ => getSelectedFile.toPath)
    .subscribe(pathSubject)


private def formatReport(report: Report): String =
  def formatBand(band: SizeBand): String = band match
    case SizeBand.Bounded(lower, upper) => s"[$lower, $upper]"
    case SizeBand.BoundedBelow(lower) => s"[$lower, ...]"
  val bandLines = report.sizeDistribution
    .map: (band, count) =>
      s"${formatBand(band)}: $count files."
    .mkString("\n")
  s"""Reporting a total of ${report.fileCount} files:
     |$bandLines
     |""".stripMargin


class Frame extends JFrame("FSStat GUI"):
  setSize(600, 400)
  setResizable(false)
  setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
  private val panel = JPanel()

  private val pathSubject = PublishSubject.create[Path]()

  private val pathButtonSubject = PublishSubject.create[Unit]()
  private val pathButton = JButton("Choose")
  pathButton.setPreferredSize(Dimension(80, 30))
  pathButton.addActionListener(_ => pathButtonSubject.onNext(()))
  pathSubject.subscribe(_ => startButton.setEnabled(true))

  private val pathChooser = ReactiveFileChooser(pathButtonSubject, pathSubject, this)
  pathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY)

  private val pathField = JTextField()
  pathField.setPreferredSize(Dimension(400, 32))
  pathField.setEditable(false)
  pathSubject.subscribe(path => pathField.setText(path.toString))

  private val reports = pathSubject
    .toFlowable(BackpressureStrategy.LATEST)
    .flatMap(path =>
      val stopper = CompletableSubject.create()
      stopSubject.subscribe(_ => stopper.onComplete())
      stopper.subscribe(() => stopSubject.onNext(()))
      val reports = FSStatLib.getFSReportInteractive(path, 100000, 10, Some(stopper))
      startSubject.subscribe(_ => reports.connect())
      reports
    )

  private val reportField = JTextArea()
  reportField.setPreferredSize(Dimension(400, 300))

  reports
    .sample(200, TimeUnit.MILLISECONDS, emitLast = true)
    .onBackpressureLatest()
    .subscribe(r => reportField.setText(formatReport(r)))

  private val startSubject = PublishSubject.create[Unit]()
  private val startButton = JButton("Start")
  startButton.setEnabled(false)
  startButton.addActionListener(_ => startSubject.onNext(()))
  startSubject.subscribe(_ => pathButton.setEnabled(false))
  startSubject.subscribe(_ => startButton.setEnabled(false))
  startSubject.subscribe(_ => stopButton.setEnabled(true))

  private val stopSubject = PublishSubject.create[Unit]()
  private val stopButton = JButton("Stop")
  stopButton.setEnabled(false)
  stopButton.addActionListener(_ => stopSubject.onNext(()))
  stopSubject.subscribe(_ => pathButton.setEnabled(true))
  stopSubject.subscribe(_ => stopButton.setEnabled(false))

  panel.add(pathField)
  panel.add(pathButton)
  panel.add(reportField)
  panel.add(startButton)
  panel.add(stopButton)
  add(panel)

  SwingUtilities.invokeLater(() => setVisible(true))


@main
def main(): Unit =
  Frame()

