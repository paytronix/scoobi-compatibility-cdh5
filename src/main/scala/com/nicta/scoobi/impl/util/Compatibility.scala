package com.nicta.scoobi.impl.util

import org.apache.hadoop.fs.{FileContext, FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.fs.Options.Rename
import java.net.URI
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

object Compatibility {

  /** @return true if the file is a directory */
  def isDirectory(fileStatus: FileStatus): Boolean =
    fileStatus.isDirectory

  /** @return true if the path is a directory */
  def isDirectory(fs: FileSystem, path: Path): Boolean =
    fs.isDirectory(path)

  /** @return the file system scheme */
  def getScheme(fs: FileSystem): String =
    fs.getScheme

  /** @return a sequence file reader */
  def newSequenceFileReader(configuration: Configuration, path: Path): SequenceFile.Reader =
    new Reader(configuration, Reader.file(path))

  /**
   * create TaskAttemptContext from a JobConf and jobId 
   */
  def newTaskAttemptContext(conf: Configuration, id: TaskAttemptID): TaskAttemptContext =
    new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(conf, id)

  /**
   * create JobContext from a configuration and jobId
   */
  def newJobContext(conf: Configuration, id: JobID): JobContext =
    new org.apache.hadoop.mapreduce.task.JobContextImpl(conf, id)

  /**
   * create a new Job from a configuration
   */
  def newJob(conf: Configuration): Job =
    org.apache.hadoop.mapreduce.Job.getInstance(conf)

  /**
   * create a new Job from a configuration and name
   */
  def newJob(conf: Configuration, name: String): Job =
    org.apache.hadoop.mapreduce.Job.getInstance(conf, name)

  /**
   * create a MapContext from a JobConf and jobId
   */
  def newMapContext(conf: Configuration, id: TaskAttemptID, reader: RecordReader[Any,Any], writer: RecordWriter[Any,Any], outputCommitter: OutputCommitter, reporter: StatusReporter, split: InputSplit): MapContext[Any,Any,Any,Any] =
    new org.apache.hadoop.mapreduce.task.MapContextImpl(conf, id, reader, writer, outputCommitter, reporter, split)

  /**
   * Rename method using the FileSystem for cdh3 and FileContext (i.e. not broken when moving directories) for cdh4 and cdh5
   */
  def rename(srcPath: Path, destPath: Path)(implicit configuration: Configuration) =
    FileContext.getFileContext(configuration).rename(srcPath, destPath, Rename.OVERWRITE)

  /**
   * Invokes Configuration() on JobContext. Works with both
   * hadoop 1 and 2.
   */
  def getConfiguration(context: JobContext): Configuration =
    context.getConfiguration

  lazy val defaultFSKeyName = "fs.defaultFS"

  lazy val cache = HadoopDistributedCache()

  case class HadoopDistributedCache() {
    lazy val CACHE_FILES = "mapreduce.job.cache.files"

    def addCacheFile(uri: URI, configuration: Configuration): Unit =
      DistributedCache.addCacheFile(uri, configuration)

    def getLocalCacheFiles(configuration: Configuration): Array[Path] =
      DistributedCache.getLocalCacheFiles(configuration)

    def getCacheFiles(configuration: Configuration): Array[URI] =
      DistributedCache.getCacheFiles(configuration)

    def createSymlink(configuration: Configuration): Unit =
      DistributedCache.createSymlink(configuration)

    def addFileToClassPath(path: Path, configuration: Configuration): Unit =
      DistributedCache.addFileToClassPath(path, configuration)
  }


  def newTaskInputOutputContext(conf: Configuration, id: TaskAttemptID): TaskInputOutputContext[Any, Any, Any, Any] = {
    val attemptId = id
    val attemptContext = Compatibility.newTaskAttemptContext(conf, attemptId)

    /**
     * Limited implementation of a task input output context for use in memory
     * It is essentially only safe to access the configuration and the job/task ids on this context
     */
    new TaskInputOutputContext[Any, Any, Any, Any] {
      def nextKeyValue: Boolean = false
      def getCurrentKey: Any = ()
      def getCurrentValue: Any = ()
      def write(key: Any, value: Any) {}
      def getOutputCommitter = new FileOutputCommitter(null, attemptContext)
      def getTaskAttemptID = attemptId
      def setStatus(msg: String) {}
      def getStatus: String = ""
      def getProgress: Float = 0.0f
      def getCounter(counterName: Enum[_]) = ???
      def getCounter(groupName: String, counterName: String) = ???

      def getArchiveClassPaths(): Array[org.apache.hadoop.fs.Path]                             = ???
      def getArchiveTimestamps(): Array[String]                                                = ???
      def getCacheArchives(): Array[java.net.URI]                                              = ???
      def getCacheFiles(): Array[java.net.URI]                                                 = ???
      def getCombinerClass(): Class[_ <: org.apache.hadoop.mapreduce.Reducer[_, _, _, _]]      = attemptContext.getCombinerClass()
      def getConfiguration(): org.apache.hadoop.conf.Configuration                             = attemptContext.getConfiguration()
      def getCredentials(): org.apache.hadoop.security.Credentials                             = attemptContext.getCredentials()
      def getFileClassPaths(): Array[org.apache.hadoop.fs.Path]                                = ???
      def getFileTimestamps(): Array[String]                                                   = ???
      def getGroupingComparator(): org.apache.hadoop.io.RawComparator[_]                       = attemptContext.getGroupingComparator()
      def getInputFormatClass(): Class[_ <: org.apache.hadoop.mapreduce.InputFormat[_, _]]     = attemptContext.getInputFormatClass()
      def getJar(): String                                                                     = attemptContext.getJar()
      def getJobID(): org.apache.hadoop.mapreduce.JobID                                        = attemptContext.getJobID()
      def getJobName(): String                                                                 = attemptContext.getJobName()
      def getJobSetupCleanupNeeded(): Boolean                                                  = ???
      def getMapOutputKeyClass(): Class[_]                                                     = attemptContext.getMapOutputKeyClass()
      def getMapOutputValueClass(): Class[_]                                                   = attemptContext.getMapOutputValueClass()
      def getMapperClass(): Class[_ <: org.apache.hadoop.mapreduce.Mapper[_, _, _, _]]         = attemptContext.getMapperClass()
      def getMaxMapAttempts(): Int                                                             = ???
      def getMaxReduceAttempts(): Int                                                          = ???
      def getNumReduceTasks(): Int                                                             = attemptContext.getNumReduceTasks()
      def getOutputFormatClass(): Class[_ <: org.apache.hadoop.mapreduce.OutputFormat[_, _]]   = attemptContext.getOutputFormatClass()
      def getOutputKeyClass(): Class[_]                                                        = attemptContext.getOutputKeyClass()
      def getOutputValueClass(): Class[_]                                                      = attemptContext.getOutputValueClass()
      def getPartitionerClass(): Class[_ <: org.apache.hadoop.mapreduce.Partitioner[_, _]]     = attemptContext.getPartitionerClass()
      def getProfileEnabled(): Boolean                                                         = ???
      def getProfileParams(): String                                                           = ???
      def getReducerClass(): Class[_ <: org.apache.hadoop.mapreduce.Reducer[_, _, _, _]]       = attemptContext.getReducerClass()
      def getSortComparator(): org.apache.hadoop.io.RawComparator[_]                           = attemptContext.getSortComparator()
      def getUser(): String                                                                    = ???
      def getWorkingDirectory(): org.apache.hadoop.fs.Path                                     = attemptContext.getWorkingDirectory()
      def progress(): Unit                                                                     = attemptContext.progress()
      // this methods are not defined on attempt context in CDH4
      def getCombinerKeyGroupingComparator(): org.apache.hadoop.io.RawComparator[_]            = ???
      def getLocalCacheArchives(): Array[org.apache.hadoop.fs.Path]                            = ???
      def getLocalCacheFiles(): Array[org.apache.hadoop.fs.Path]                               = ???
      def getProfileTaskRange(x$1: Boolean): org.apache.hadoop.conf.Configuration.IntegerRanges= ???
      def getSymlink(): Boolean                                                                = ???
      def getTaskCleanupNeeded(): Boolean                                                      = ???
      def userClassesTakesPrecedence(): Boolean                                                = ???

    }
  }
}
