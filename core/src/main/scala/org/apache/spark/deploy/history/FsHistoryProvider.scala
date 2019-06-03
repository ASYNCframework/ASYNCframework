/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import java.io.{File, FileNotFoundException, IOException}
import java.util.{Date, ServiceLoader, UUID}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Future, TimeUnit}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionException
import scala.util.Try
import scala.xml.Node

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.common.io.ByteStreams
import com.google.common.util.concurrent.MoreExecutors
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.hadoop.security.AccessControlException
import org.fusesource.leveldbjni.internal.NativeDB

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.status._
import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.status.config._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}
import org.apache.spark.util.kvstore._

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider checks for new finished applications in the background periodically and
 * renders the history application UI by parsing the associated event logs.
 *
 * == How new and updated attempts are detected ==
 *
 * - New attempts are detected in [[checkForLogs]]: the log dir is scanned, and any
 * entries in the log dir whose modification time is greater than the last scan time
 * are considered new or updated. These are replayed to create a new attempt info entry
 * and update or create a matching application info element in the list of applications.
 * - Updated attempts are also found in [[checkForLogs]] -- if the attempt's log file has grown, the
 * attempt is replaced by another one with a larger log size.
 *
 * The use of log size, rather than simply relying on modification times, is needed to
 * address the following issues
 * - some filesystems do not appear to update the `modtime` value whenever data is flushed to
 * an open file output stream. Changes to the history may not be picked up.
 * - the granularity of the `modtime` field may be 2+ seconds. Rapid changes to the FS can be
 * missed.
 *
 * Tracking filesize works given the following invariant: the logs get bigger
 * as new events are added. If a format was used in which this did not hold, the mechanism would
 * break. Simple streaming of JSON-formatted events, as is implemented today, implicitly
 * maintains this invariant.
 */
private[history] class FsHistoryProvider(conf: SparkConf, clock: Clock)
  extends ApplicationHistoryProvider with Logging {

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  import config._
  import FsHistoryProvider._

  // Interval between safemode checks.
  private val SAFEMODE_CHECK_INTERVAL_S = conf.getTimeAsSeconds(
    "spark.history.fs.safemodeCheck.interval", "5s")

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.update.interval", "10s")

  // Interval between each cleaner checks for event logs to delete
  private val CLEAN_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.cleaner.interval", "1d")

  // Number of threads used to replay event logs.
  private val NUM_PROCESSING_THREADS = conf.getInt(SPARK_HISTORY_FS_NUM_REPLAY_THREADS,
    Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  private val logDir = conf.get(EVENT_LOG_DIR)

  private val HISTORY_UI_ACLS_ENABLE = conf.getBoolean("spark.history.ui.acls.enable", false)
  private val HISTORY_UI_ADMIN_ACLS = conf.get("spark.history.ui.admin.acls", "")
  private val HISTORY_UI_ADMIN_ACLS_GROUPS = conf.get("spark.history.ui.admin.acls.groups", "")
  logInfo(s"History server ui acls " + (if (HISTORY_UI_ACLS_ENABLE) "enabled" else "disabled") +
    "; users with admin permissions: " + HISTORY_UI_ADMIN_ACLS.toString +
    "; groups with admin permissions" + HISTORY_UI_ADMIN_ACLS_GROUPS.toString)

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  // Visible for testing
  private[history] val fs: FileSystem = new Path(logDir).getFileSystem(hadoopConf)

  // Used by check event thread and clean log thread.
  // Scheduled thread pool size must be one, otherwise it will have concurrent issues about fs
  // and applications between check task and clean task.
  private val pool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-history-task-%d")

  // The modification time of the newest log detected during the last scan.   Currently only
  // used for logging msgs (logs are re-scanned based on file size, rather than modtime)
  private val lastScanTime = new java.util.concurrent.atomic.AtomicLong(-1)

  private val pendingReplayTasksCount = new java.util.concurrent.atomic.AtomicInteger(0)

  private val storePath = conf.get(LOCAL_STORE_DIR).map(new File(_))

  // Visible for testing.
  private[history] val listing: KVStore = storePath.map { path =>
    require(path.isDirectory(), s"Configured store directory ($path) does not exist.")
    val dbPath = new File(path, "listing.ldb")
    val metadata = new FsHistoryProviderMetadata(CURRENT_LISTING_VERSION,
      AppStatusStore.CURRENT_VERSION, logDir.toString())

    try {
      open(dbPath, metadata)
    } catch {
      // If there's an error, remove the listing database and any existing UI database
      // from the store directory, since it's extremely likely that they'll all contain
      // incompatible information.
      case _: UnsupportedStoreVersionException | _: MetadataMismatchException =>
        logInfo("Detected incompatible DB versions, deleting...")
        path.listFiles().foreach(Utils.deleteRecursively)
        open(dbPath, metadata)
      case dbExc: NativeDB.DBException =>
        // Get rid of the corrupted listing.ldb and re-create it.
        logWarning(s"Failed to load disk store $dbPath :", dbExc)
        Utils.deleteRecursively(dbPath)
        open(dbPath, metadata)
    }
  }.getOrElse(new InMemoryStore())

  private val diskManager = storePath.map { path =>
    new HistoryServerDiskManager(conf, path, listing, clock)
  }

  private val blacklist = new ConcurrentHashMap[String, Long]

  // Visible for testing
  private[history] def isBlacklisted(path: Path): Boolean = {
    blacklist.containsKey(path.getName)
  }

  private def blacklist(path: Path): Unit = {
    blacklist.put(path.getName, clock.getTimeMillis())
  }

  /**
   * Removes expired entries in the blacklist, according to the provided `expireTimeInSeconds`.
   */
  private def clearBlacklist(expireTimeInSeconds: Long): Unit = {
    val expiredThreshold = clock.getTimeMillis() - expireTimeInSeconds * 1000
    blacklist.asScala.retain((_, creationTime) => creationTime >= expiredThreshold)
  }

  private val activeUIs = new mutable.HashMap[(String, Option[String]), LoadedAppUI]()

  /**
   * Return a runnable that performs the given operation on the event logs.
   * This operation is expected to be executed periodically.
   */
  private def getRunner(operateFun: () => Unit): Runnable = {
    new Runnable() {
      override def run(): Unit = Utils.tryOrExit {
        operateFun()
      }
    }
  }

  /**
   * Fixed size thread pool to fetch and parse log files.
   */
  private val replayExecutor: ExecutorService = {
    if (!Utils.isTesting) {
      ThreadUtils.newDaemonFixedThreadPool(NUM_PROCESSING_THREADS, "log-replay-executor")
    } else {
      MoreExecutors.sameThreadExecutor()
    }
  }

  val initThread = initialize()

  private[history] def initialize(): Thread = {
    if (!isFsInSafeMode()) {
      startPolling()
      null
    } else {
      startSafeModeCheckThread(None)
    }
  }

  private[history] def startSafeModeCheckThread(
      errorHandler: Option[Thread.UncaughtExceptionHandler]): Thread = {
    // Cannot probe anything while the FS is in safe mode, so spawn a new thread that will wait
    // for the FS to leave safe mode before enabling polling. This allows the main history server
    // UI to be shown (so that the user can see the HDFS status).
    val initThread = new Thread(new Runnable() {
      override def run(): Unit = {
        try {
          while (isFsInSafeMode()) {
            logInfo("HDFS is still in safe mode. Waiting...")
            val deadline = clock.getTimeMillis() +
              TimeUnit.SECONDS.toMillis(SAFEMODE_CHECK_INTERVAL_S)
            clock.waitTillTime(deadline)
          }
          startPolling()
        } catch {
          case _: InterruptedException =>
        }
      }
    })
    initThread.setDaemon(true)
    initThread.setName(s"${getClass().getSimpleName()}-init")
    initThread.setUncaughtExceptionHandler(errorHandler.getOrElse(
      new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError("Error initializing FsHistoryProvider.", e)
          System.exit(1)
        }
      }))
    initThread.start()
    initThread
  }

  private def startPolling(): Unit = {
    diskManager.foreach(_.initialize())

    // Validate the log directory.
    val path = new Path(logDir)
    try {
      if (!fs.getFileStatus(path).isDirectory) {
        throw new IllegalArgumentException(
          "Logging directory specified is not a directory: %s".format(logDir))
      }
    } catch {
      case f: FileNotFoundException =>
        var msg = s"Log directory specified does not exist: $logDir"
        if (logDir == DEFAULT_LOG_DIR) {
          msg += " Did you configure the correct one through spark.history.fs.logDirectory?"
        }
        throw new FileNotFoundException(msg).initCause(f)
    }

    // Disable the background thread during tests.
    if (!conf.contains("spark.testing")) {
      // A task that periodically checks for event log updates on disk.
      logDebug(s"Scheduling update thread every $UPDATE_INTERVAL_S seconds")
      pool.scheduleWithFixedDelay(
        getRunner(() => checkForLogs()), 0, UPDATE_INTERVAL_S, TimeUnit.SECONDS)

      if (conf.getBoolean("spark.history.fs.cleaner.enabled", false)) {
        // A task that periodically cleans event logs on disk.
        pool.scheduleWithFixedDelay(
          getRunner(() => cleanLogs()), 0, CLEAN_INTERVAL_S, TimeUnit.SECONDS)
      }
    } else {
      logDebug("Background update thread disabled for testing")
    }
  }

  override def getListing(): Iterator[ApplicationInfo] = {
    // Return the listing in end time descending order.
    listing.view(classOf[ApplicationInfoWrapper])
      .index("endTime")
      .reverse()
      .iterator()
      .asScala
      .map(_.toApplicationInfo())
  }

  override def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    try {
      Some(load(appId).toApplicationInfo())
    } catch {
      case _: NoSuchElementException =>
        None
    }
  }

  override def getEventLogsUnderProcess(): Int = pendingReplayTasksCount.get()

  override def getLastUpdatedTime(): Long = lastScanTime.get()

  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    val app = try {
      load(appId)
     } catch {
      case _: NoSuchElementException =>
        return None
    }

    val attempt = app.attempts.find(_.info.attemptId == attemptId).orNull
    if (attempt == null) {
      return None
    }

    val conf = this.conf.clone()
    val secManager = new SecurityManager(conf)

    secManager.setAcls(HISTORY_UI_ACLS_ENABLE)
    // make sure to set admin acls before view acls so they are properly picked up
    secManager.setAdminAcls(HISTORY_UI_ADMIN_ACLS + "," + attempt.adminAcls.getOrElse(""))
    secManager.setViewAcls(attempt.info.sparkUser, attempt.viewAcls.getOrElse(""))
    secManager.setAdminAclsGroups(HISTORY_UI_ADMIN_ACLS_GROUPS + "," +
      attempt.adminAclsGroups.getOrElse(""))
    secManager.setViewAclsGroups(attempt.viewAclsGroups.getOrElse(""))

    val kvstore = try {
      diskManager match {
        case Some(sm) =>
          loadDiskStore(sm, appId, attempt)

        case _ =>
          createInMemoryStore(attempt)
      }
    } catch {
      case _: FileNotFoundException =>
        return None
    }

    val ui = SparkUI.create(None, new AppStatusStore(kvstore), conf, secManager, app.info.name,
      HistoryServer.getAttemptURI(appId, attempt.info.attemptId),
      attempt.info.startTime.getTime(),
      attempt.info.appSparkVersion)
    loadPlugins().foreach(_.setupUI(ui))

    val loadedUI = LoadedAppUI(ui)

    synchronized {
      activeUIs((appId, attemptId)) = loadedUI
    }

    Some(loadedUI)
  }

  override def getEmptyListingHtml(): Seq[Node] = {
    <p>
      Did you specify the correct logging directory? Please verify your setting of
      <span style="font-style:italic">spark.history.fs.logDirectory</span>
      listed above and whether you have the permissions to access it.
      <br/>
      It is also possible that your application did not run to
      completion or did not stop the SparkContext.
    </p>
  }

  override def getConfig(): Map[String, String] = {
    val safeMode = if (isFsInSafeMode()) {
      Map("HDFS State" -> "In safe mode, application logs not available.")
    } else {
      Map()
    }
    Map("Event log directory" -> logDir.toString) ++ safeMode
  }

  override def stop(): Unit = {
    try {
      if (initThread != null && initThread.isAlive()) {
        initThread.interrupt()
        initThread.join()
      }
      Seq(pool, replayExecutor).foreach { executor =>
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow()
        }
      }
    } finally {
      activeUIs.foreach { case (_, loadedUI) => loadedUI.ui.store.close() }
      activeUIs.clear()
      listing.close()
    }
  }

  override def onUIDetached(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
    val uiOption = synchronized {
      activeUIs.remove((appId, attemptId))
    }
    uiOption.foreach { loadedUI =>
      loadedUI.lock.writeLock().lock()
      try {
        loadedUI.ui.store.close()
      } finally {
        loadedUI.lock.writeLock().unlock()
      }

      diskManager.foreach { dm =>
        // If the UI is not valid, delete its files from disk, if any. This relies on the fact that
        // ApplicationCache will never call this method concurrently with getAppUI() for the same
        // appId / attemptId.
        dm.release(appId, attemptId, delete = !loadedUI.valid)
      }
    }
  }

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   */
  private[history] def checkForLogs(): Unit = {
    try {
      val newLastScanTime = getNewLastScanTime()
      logDebug(s"Scanning $logDir with lastScanTime==$lastScanTime")

      val updated = Option(fs.listStatus(new Path(logDir))).map(_.toSeq).getOrElse(Nil)
        .filter { entry =>
          !entry.isDirectory() &&
            // FsHistoryProvider generates a hidden file which can't be read.  Accidentally
            // reading a garbage file is safe, but we would log an error which can be scary to
            // the end-user.
            !entry.getPath().getName().startsWith(".") &&
            !isBlacklisted(entry.getPath)
        }
        .filter { entry =>
          try {
            val info = listing.read(classOf[LogInfo], entry.getPath().toString())
            if (info.fileSize < entry.getLen()) {
              // Log size has changed, it should be parsed.
              true
            } else {
              // If the SHS view has a valid application, update the time the file was last seen so
              // that the entry is not deleted from the SHS listing.
              if (info.appId.isDefined) {
                listing.write(info.copy(lastProcessed = newLastScanTime))
              }
              false
            }
          } catch {
            case _: NoSuchElementException =>
              // If the file is currently not being tracked by the SHS, add an entry for it and try
              // to parse it. This will allow the cleaner code to detect the file as stale later on
              // if it was not possible to parse it.
              listing.write(LogInfo(entry.getPath().toString(), newLastScanTime, None, None,
                entry.getLen()))
              entry.getLen() > 0
          }
        }
        .sortWith { case (entry1, entry2) =>
          entry1.getModificationTime() > entry2.getModificationTime()
        }

      if (updated.nonEmpty) {
        logDebug(s"New/updated attempts found: ${updated.size} ${updated.map(_.getPath)}")
      }

      val tasks = updated.flatMap { entry =>
        try {
          val task: Future[Unit] = replayExecutor.submit(new Runnable {
            override def run(): Unit = mergeApplicationListing(entry, newLastScanTime)
          }, Unit)
          Some(task -> entry.getPath)
        } catch {
          // let the iteration over the updated entries break, since an exception on
          // replayExecutor.submit (..) indicates the ExecutorService is unable
          // to take any more submissions at this time
          case e: Exception =>
            logError(s"Exception while submitting event log for replay", e)
            None
        }
      }

      pendingReplayTasksCount.addAndGet(tasks.size)

      // Wait for all tasks to finish. This makes sure that checkForLogs
      // is not scheduled again while some tasks are already running in
      // the replayExecutor.
      tasks.foreach { case (task, path) =>
        try {
          task.get()
        } catch {
          case e: InterruptedException =>
            throw e
          case e: ExecutionException if e.getCause.isInstanceOf[AccessControlException] =>
            // We don't have read permissions on the log file
            logWarning(s"Unable to read log $path", e.getCause)
            blacklist(path)
          case e: Exception =>
            logError("Exception while merging application listings", e)
        } finally {
          pendingReplayTasksCount.decrementAndGet()
        }
      }

      // Delete all information about applications whose log files disappeared from storage.
      // This is done by identifying the event logs which were not touched by the current
      // directory scan.
      //
      // Only entries with valid applications are cleaned up here. Cleaning up invalid log
      // files is done by the periodic cleaner task.
      val stale = listing.view(classOf[LogInfo])
        .index("lastProcessed")
        .last(newLastScanTime - 1)
        .asScala
        .toList
      stale.foreach { log =>
        log.appId.foreach { appId =>
          cleanAppData(appId, log.attemptId, log.logPath)
          listing.delete(classOf[LogInfo], log.logPath)
        }
      }

      lastScanTime.set(newLastScanTime)
    } catch {
      case e: Exception => logError("Exception in checking for event log updates", e)
    }
  }

  private def cleanAppData(appId: String, attemptId: Option[String], logPath: String): Unit = {
    try {
      val app = load(appId)
      val (attempt, others) = app.attempts.partition(_.info.attemptId == attemptId)

      assert(attempt.isEmpty || attempt.size == 1)
      val isStale = attempt.headOption.exists { a =>
        if (a.logPath != new Path(logPath).getName()) {
          // If the log file name does not match, then probably the old log file was from an
          // in progress application. Just return that the app should be left alone.
          false
        } else {
          val maybeUI = synchronized {
            activeUIs.remove(appId -> attemptId)
          }

          maybeUI.foreach { ui =>
            ui.invalidate()
            ui.ui.store.close()
          }

          diskManager.foreach(_.release(appId, attemptId, delete = true))
          true
        }
      }

      if (isStale) {
        if (others.nonEmpty) {
          val newAppInfo = new ApplicationInfoWrapper(app.info, others)
          listing.write(newAppInfo)
        } else {
          listing.delete(classOf[ApplicationInfoWrapper], appId)
        }
      }
    } catch {
      case _: NoSuchElementException =>
    }
  }

  private[history] def getNewLastScanTime(): Long = {
    val fileName = "." + UUID.randomUUID().toString
    val path = new Path(logDir, fileName)
    val fos = fs.create(path)

    try {
      fos.close()
      fs.getFileStatus(path).getModificationTime
    } catch {
      case e: Exception =>
        logError("Exception encountered when attempting to update last scan time", e)
        lastScanTime.get()
    } finally {
      if (!fs.delete(path, true)) {
        logWarning(s"Error deleting ${path}")
      }
    }
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {

    /**
     * This method compresses the files passed in, and writes the compressed data out into the
     * [[OutputStream]] passed in. Each file is written as a new [[ZipEntry]] with its name being
     * the name of the file being compressed.
     */
    def zipFileToStream(file: Path, entryName: String, outputStream: ZipOutputStream): Unit = {
      val fs = file.getFileSystem(hadoopConf)
      val inputStream = fs.open(file, 1 * 1024 * 1024) // 1MB Buffer
      try {
        outputStream.putNextEntry(new ZipEntry(entryName))
        ByteStreams.copy(inputStream, outputStream)
        outputStream.closeEntry()
      } finally {
        inputStream.close()
      }
    }

    val app = try {
      load(appId)
    } catch {
      case _: NoSuchElementException =>
        throw new SparkException(s"Logs for $appId not found.")
    }

    try {
      // If no attempt is specified, or there is no attemptId for attempts, return all attempts
      attemptId
        .map { id => app.attempts.filter(_.info.attemptId == Some(id)) }
        .getOrElse(app.attempts)
        .map(_.logPath)
        .foreach { log =>
          zipFileToStream(new Path(logDir, log), log, zipStream)
        }
    } finally {
      zipStream.close()
    }
  }

  /**
   * Replay the given log file, saving the application in the listing db.
   */
  protected def mergeApplicationListing(fileStatus: FileStatus, scanTime: Long): Unit = {
    val eventsFilter: ReplayEventsFilter = { eventString =>
      eventString.startsWith(APPL_START_EVENT_PREFIX) ||
        eventString.startsWith(APPL_END_EVENT_PREFIX) ||
        eventString.startsWith(LOG_START_EVENT_PREFIX) ||
        eventString.startsWith(ENV_UPDATE_EVENT_PREFIX)
    }

    val logPath = fileStatus.getPath()
    val bus = new ReplayListenerBus()
    val listener = new AppListingListener(fileStatus, clock)
    bus.addListener(listener)
    replay(fileStatus, bus, eventsFilter = eventsFilter)

    val (appId, attemptId) = listener.applicationInfo match {
      case Some(app) =>
        // Invalidate the existing UI for the reloaded app attempt, if any. See LoadedAppUI for a
        // discussion on the UI lifecycle.
        synchronized {
          activeUIs.get((app.info.id, app.attempts.head.info.attemptId)).foreach { ui =>
            ui.invalidate()
            ui.ui.store.close()
          }
        }

        addListing(app)
        (Some(app.info.id), app.attempts.head.info.attemptId)

      case _ =>
        // If the app hasn't written down its app ID to the logs, still record the entry in the
        // listing db, with an empty ID. This will make the log eligible for deletion if the app
        // does not make progress after the configured max log age.
        (None, None)
    }
    listing.write(LogInfo(logPath.toString(), scanTime, appId, attemptId, fileStatus.getLen()))
  }

  /**
   * Delete event logs from the log directory according to the clean policy defined by the user.
   */
  private[history] def cleanLogs(): Unit = Utils.tryLog {
    val maxTime = clock.getTimeMillis() - conf.get(MAX_LOG_AGE_S) * 1000

    val expired = listing.view(classOf[ApplicationInfoWrapper])
      .index("oldestAttempt")
      .reverse()
      .first(maxTime)
      .asScala
      .toList
    expired.foreach { app =>
      // Applications may have multiple attempts, some of which may not need to be deleted yet.
      val (remaining, toDelete) = app.attempts.partition { attempt =>
        attempt.info.lastUpdated.getTime() >= maxTime
      }

      if (remaining.nonEmpty) {
        val newApp = new ApplicationInfoWrapper(app.info, remaining)
        listing.write(newApp)
      }

      toDelete.foreach { attempt =>
        logInfo(s"Deleting expired event log for ${attempt.logPath}")
        val logPath = new Path(logDir, attempt.logPath)
        listing.delete(classOf[LogInfo], logPath.toString())
        cleanAppData(app.id, attempt.info.attemptId, logPath.toString())
        deleteLog(logPath)
      }

      if (remaining.isEmpty) {
        listing.delete(app.getClass(), app.id)
      }
    }

    // Delete log files that don't have a valid application and exceed the configured max age.
    val stale = listing.view(classOf[LogInfo])
      .index("lastProcessed")
      .reverse()
      .first(maxTime)
      .asScala
      .toList
    stale.foreach { log =>
      if (log.appId.isEmpty) {
        logInfo(s"Deleting invalid / corrupt event log ${log.logPath}")
        deleteLog(new Path(log.logPath))
        listing.delete(classOf[LogInfo], log.logPath)
      }
    }
    // Clean the blacklist from the expired entries.
    clearBlacklist(CLEAN_INTERVAL_S)
  }

  /**
   * Replays the events in the specified log file on the supplied `ReplayListenerBus`.
   * `ReplayEventsFilter` determines what events are replayed.
   */
  private def replay(
      eventLog: FileStatus,
      bus: ReplayListenerBus,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Unit = {
    val logPath = eventLog.getPath()
    val isCompleted = !logPath.getName().endsWith(EventLoggingListener.IN_PROGRESS)
    logInfo(s"Replaying log path: $logPath")
    // Note that the eventLog may have *increased* in size since when we grabbed the filestatus,
    // and when we read the file here.  That is OK -- it may result in an unnecessary refresh
    // when there is no update, but will not result in missing an update.  We *must* prevent
    // an error the other way -- if we report a size bigger (ie later) than the file that is
    // actually read, we may never refresh the app.  FileStatus is guaranteed to be static
    // after it's created, so we get a file size that is no bigger than what is actually read.
    Utils.tryWithResource(EventLoggingListener.openEventLog(logPath, fs)) { in =>
      bus.replay(in, logPath.toString, !isCompleted, eventsFilter)
      logInfo(s"Finished parsing $logPath")
    }
  }

  /**
   * Rebuilds the application state store from its event log.
   */
  private def rebuildAppStore(
      store: KVStore,
      eventLog: FileStatus,
      lastUpdated: Long): Unit = {
    // Disable async updates, since they cause higher memory usage, and it's ok to take longer
    // to parse the event logs in the SHS.
    val replayConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
    val trackingStore = new ElementTrackingStore(store, replayConf)
    val replayBus = new ReplayListenerBus()
    val listener = new AppStatusListener(trackingStore, replayConf, false,
      lastUpdateTime = Some(lastUpdated))
    replayBus.addListener(listener)

    for {
      plugin <- loadPlugins()
      listener <- plugin.createListeners(conf, trackingStore)
    } replayBus.addListener(listener)

    try {
      replay(eventLog, replayBus)
      trackingStore.close(false)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
  }

  /**
   * Checks whether HDFS is in safe mode.
   *
   * Note that DistributedFileSystem is a `@LimitedPrivate` class, which for all practical reasons
   * makes it more public than not.
   */
  private[history] def isFsInSafeMode(): Boolean = fs match {
    case dfs: DistributedFileSystem =>
      isFsInSafeMode(dfs)
    case _ =>
      false
  }

  private[history] def isFsInSafeMode(dfs: DistributedFileSystem): Boolean = {
    /* true to check only for Active NNs status */
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, true)
  }

  /**
   * String description for diagnostics
   * @return a summary of the component state
   */
  override def toString: String = {
    val count = listing.count(classOf[ApplicationInfoWrapper])
    s"""|FsHistoryProvider{logdir=$logDir,
        |  storedir=$storePath,
        |  last scan time=$lastScanTime
        |  application count=$count}""".stripMargin
  }

  private def load(appId: String): ApplicationInfoWrapper = {
    listing.read(classOf[ApplicationInfoWrapper], appId)
  }

  /**
   * Write the app's information to the given store. Serialized to avoid the (notedly rare) case
   * where two threads are processing separate attempts of the same application.
   */
  private def addListing(app: ApplicationInfoWrapper): Unit = listing.synchronized {
    val attempt = app.attempts.head

    val oldApp = try {
      load(app.id)
    } catch {
      case _: NoSuchElementException =>
        app
    }

    def compareAttemptInfo(a1: AttemptInfoWrapper, a2: AttemptInfoWrapper): Boolean = {
      a1.info.startTime.getTime() > a2.info.startTime.getTime()
    }

    val attempts = oldApp.attempts.filter(_.info.attemptId != attempt.info.attemptId) ++
      List(attempt)

    val newAppInfo = new ApplicationInfoWrapper(
      app.info,
      attempts.sortWith(compareAttemptInfo))
    listing.write(newAppInfo)
  }

  private def loadDiskStore(
      dm: HistoryServerDiskManager,
      appId: String,
      attempt: AttemptInfoWrapper): KVStore = {
    val metadata = new AppStatusStoreMetadata(AppStatusStore.CURRENT_VERSION)

    // First check if the store already exists and try to open it. If that fails, then get rid of
    // the existing data.
    dm.openStore(appId, attempt.info.attemptId).foreach { path =>
      try {
        return KVUtils.open(path, metadata)
      } catch {
        case e: Exception =>
          logInfo(s"Failed to open existing store for $appId/${attempt.info.attemptId}.", e)
          dm.release(appId, attempt.info.attemptId, delete = true)
      }
    }

    // At this point the disk data either does not exist or was deleted because it failed to
    // load, so the event log needs to be replayed.
    val status = fs.getFileStatus(new Path(logDir, attempt.logPath))
    val isCompressed = EventLoggingListener.codecName(status.getPath()).flatMap { name =>
      Try(CompressionCodec.getShortName(name)).toOption
    }.isDefined
    logInfo(s"Leasing disk manager space for app $appId / ${attempt.info.attemptId}...")
    val lease = dm.lease(status.getLen(), isCompressed)
    val newStorePath = try {
      Utils.tryWithResource(KVUtils.open(lease.tmpPath, metadata)) { store =>
        rebuildAppStore(store, status, attempt.info.lastUpdated.getTime())
      }
      lease.commit(appId, attempt.info.attemptId)
    } catch {
      case e: Exception =>
        lease.rollback()
        throw e
    }

    KVUtils.open(newStorePath, metadata)
  }

  private def createInMemoryStore(attempt: AttemptInfoWrapper): KVStore = {
    val store = new InMemoryStore()
    val status = fs.getFileStatus(new Path(logDir, attempt.logPath))
    rebuildAppStore(store, status, attempt.info.lastUpdated.getTime())
    store
  }

  private def loadPlugins(): Iterable[AppHistoryServerPlugin] = {
    ServiceLoader.load(classOf[AppHistoryServerPlugin], Utils.getContextOrSparkClassLoader).asScala
  }

  /** For testing. Returns internal data about a single attempt. */
  private[history] def getAttempt(appId: String, attemptId: Option[String]): AttemptInfoWrapper = {
    load(appId).attempts.find(_.info.attemptId == attemptId).getOrElse(
      throw new NoSuchElementException(s"Cannot find attempt $attemptId of $appId."))
  }

  private def deleteLog(log: Path): Unit = {
    if (isBlacklisted(log)) {
      logDebug(s"Skipping deleting $log as we don't have permissions on it.")
    } else {
      try {
        fs.delete(log, true)
      } catch {
        case _: AccessControlException =>
          logInfo(s"No permission to delete $log, ignoring.")
        case ioe: IOException =>
          logError(s"IOException in cleaning $log", ioe)
      }
    }
  }

}

private[history] object FsHistoryProvider {
  private val SPARK_HISTORY_FS_NUM_REPLAY_THREADS = "spark.history.fs.numReplayThreads"

  private val APPL_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationStart\""

  private val APPL_END_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationEnd\""

  private val LOG_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerLogStart\""

  private val ENV_UPDATE_EVENT_PREFIX = "{\"Event\":\"SparkListenerEnvironmentUpdate\","

  /**
   * Current version of the data written to the listing database. When opening an existing
   * db, if the version does not match this value, the FsHistoryProvider will throw away
   * all data and re-generate the listing data from the event logs.
   */
  private[history] val CURRENT_LISTING_VERSION = 1L
}

private[history] case class FsHistoryProviderMetadata(
    version: Long,
    uiVersion: Long,
    logDir: String)

/**
 * Tracking info for event logs detected in the configured log directory. Tracks both valid and
 * invalid logs (e.g. unparseable logs, recorded as logs with no app ID) so that the cleaner
 * can know what log files are safe to delete.
 */
private[history] case class LogInfo(
    @KVIndexParam logPath: String,
    @KVIndexParam("lastProcessed") lastProcessed: Long,
    appId: Option[String],
    attemptId: Option[String],
    fileSize: Long)

private[history] class AttemptInfoWrapper(
    val info: ApplicationAttemptInfo,
    val logPath: String,
    val fileSize: Long,
    val adminAcls: Option[String],
    val viewAcls: Option[String],
    val adminAclsGroups: Option[String],
    val viewAclsGroups: Option[String])

private[history] class ApplicationInfoWrapper(
    val info: ApplicationInfo,
    val attempts: List[AttemptInfoWrapper]) {

  @JsonIgnore @KVIndexParam
  def id: String = info.id

  @JsonIgnore @KVIndexParam("endTime")
  def endTime(): Long = attempts.head.info.endTime.getTime()

  @JsonIgnore @KVIndexParam("oldestAttempt")
  def oldestAttempt(): Long = attempts.map(_.info.lastUpdated.getTime()).min

  def toApplicationInfo(): ApplicationInfo = info.copy(attempts = attempts.map(_.info))

}

private[history] class AppListingListener(log: FileStatus, clock: Clock) extends SparkListener {

  private val app = new MutableApplicationInfo()
  private val attempt = new MutableAttemptInfo(log.getPath().getName(), log.getLen())

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    app.id = event.appId.orNull
    app.name = event.appName

    attempt.attemptId = event.appAttemptId
    attempt.startTime = new Date(event.time)
    attempt.lastUpdated = new Date(clock.getTimeMillis())
    attempt.sparkUser = event.sparkUser
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    attempt.endTime = new Date(event.time)
    attempt.lastUpdated = new Date(log.getModificationTime())
    attempt.duration = event.time - attempt.startTime.getTime()
    attempt.completed = true
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    val allProperties = event.environmentDetails("Spark Properties").toMap
    attempt.viewAcls = allProperties.get("spark.ui.view.acls")
    attempt.adminAcls = allProperties.get("spark.admin.acls")
    attempt.viewAclsGroups = allProperties.get("spark.ui.view.acls.groups")
    attempt.adminAclsGroups = allProperties.get("spark.admin.acls.groups")
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(sparkVersion) =>
      attempt.appSparkVersion = sparkVersion
    case _ =>
  }

  def applicationInfo: Option[ApplicationInfoWrapper] = {
    if (app.id != null) {
      Some(app.toView())
    } else {
      None
    }
  }

  private class MutableApplicationInfo {
    var id: String = null
    var name: String = null
    var coresGranted: Option[Int] = None
    var maxCores: Option[Int] = None
    var coresPerExecutor: Option[Int] = None
    var memoryPerExecutorMB: Option[Int] = None

    def toView(): ApplicationInfoWrapper = {
      val apiInfo = ApplicationInfo(id, name, coresGranted, maxCores, coresPerExecutor,
        memoryPerExecutorMB, Nil)
      new ApplicationInfoWrapper(apiInfo, List(attempt.toView()))
    }

  }

  private class MutableAttemptInfo(logPath: String, fileSize: Long) {
    var attemptId: Option[String] = None
    var startTime = new Date(-1)
    var endTime = new Date(-1)
    var lastUpdated = new Date(-1)
    var duration = 0L
    var sparkUser: String = null
    var completed = false
    var appSparkVersion = ""

    var adminAcls: Option[String] = None
    var viewAcls: Option[String] = None
    var adminAclsGroups: Option[String] = None
    var viewAclsGroups: Option[String] = None

    def toView(): AttemptInfoWrapper = {
      val apiInfo = ApplicationAttemptInfo(
        attemptId,
        startTime,
        endTime,
        lastUpdated,
        duration,
        sparkUser,
        completed,
        appSparkVersion)
      new AttemptInfoWrapper(
        apiInfo,
        logPath,
        fileSize,
        adminAcls,
        viewAcls,
        adminAclsGroups,
        viewAclsGroups)
    }

  }

}
