package pcd.fsstatlib

import io.reactivex.rxjava3.core.{Observable, Observer}
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.{CompletableSubject, PublishSubject}

import java.awt.{Component, Dimension}
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import javax.swing.*


val EDT_SCHEDULER = Schedulers.from(SwingUtilities.invokeLater(_))


class ReactiveFileChooser(showObservable: Observable[Unit],
                          pathObserver: Observer[Path],
                          parent: Component) extends JFileChooser:

  private val dialog = createDialog(parent)
  setDialogType(JFileChooser.OPEN_DIALOG)
  dialog.setModal(true)

  showObservable
    .observeOn(EDT_SCHEDULER)
    .subscribe(_ => dialog.setVisible(true))

  private val commandSubject = PublishSubject.create[String]()
  addActionListener(e => commandSubject.onNext(e.getActionCommand))

  commandSubject
    .observeOn(EDT_SCHEDULER)
    .subscribe(_ => dialog.setVisible(false))

  commandSubject
    .filter(JFileChooser.APPROVE_SELECTION == _)
    .map(_ => getSelectedFile.toPath)
    .subscribe(pathObserver)


class Frame extends JFrame("FSStat GUI"):

  private val upperSize = 100_000
  private val boundedBandCount = 10

  setSize(600, 400)
  setResizable(false)
  setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
  private val panel = JPanel()

  private val pathButtonSubject = PublishSubject.create[Unit]()
  private val pathButton = JButton("Choose")
  pathButton.setPreferredSize(Dimension(80, 30))
  pathButton.addActionListener(_ => pathButtonSubject.onNext(()))

  private val pathSubject = PublishSubject.create[Path]()
  private val pathChooser = ReactiveFileChooser(pathButtonSubject, pathSubject, this)
  pathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY)
  pathSubject
    .observeOn(EDT_SCHEDULER)
    .subscribe(path =>
      pathField.setText(path.toString)
      reportField.setText("")
      startButton.setEnabled(true)
    )
  pathSubject
    .subscribe(path =>
      val stopper = CompletableSubject.create()
      stopSubject.subscribe(_ => stopper.onComplete())
      stopper.subscribe(() => stopSubject.onNext(()))
      val reports = FSStatLib.getFSReportInteractive(path, upperSize, boundedBandCount, Some(stopper))
      startSubject.subscribe(_ => reports.connect())
      reports
        .observeOn(EDT_SCHEDULER)
        .sample(100, TimeUnit.MILLISECONDS, emitLast = true)
        .subscribe(
          r => reportField.setText(r.formatToString),
          e => reportField.setText(s"Failed to access the starting directory:\n$e")
        )
    )

  private val pathField = JTextField()
  pathField.setPreferredSize(Dimension(400, 32))
  pathField.setEditable(false)

  private val reportField = JTextArea()
  reportField.setPreferredSize(Dimension(400, 300))

  private val startSubject = PublishSubject.create[Unit]()
  private val startButton = JButton("Start")
  startButton.setEnabled(false)
  startButton.addActionListener(_ => startSubject.onNext(()))
  startSubject
    .observeOn(EDT_SCHEDULER)
    .subscribe(_ =>
      pathButton.setEnabled(false)
      startButton.setEnabled(false)
      stopButton.setEnabled(true)
    )

  private val stopSubject = PublishSubject.create[Unit]()
  private val stopButton = JButton("Stop")
  stopButton.setEnabled(false)
  stopButton.addActionListener(_ => stopSubject.onNext(()))
  stopSubject
    .observeOn(EDT_SCHEDULER)
    .subscribe(_ =>
      pathButton.setEnabled(true)
      stopButton.setEnabled(false)
    )

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

