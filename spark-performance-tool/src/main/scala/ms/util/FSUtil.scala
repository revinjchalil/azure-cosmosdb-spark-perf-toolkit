package ms.util

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.internal.Logging
import scala.io.Source.fromInputStream

/**
 * FS utils to read and write files from FS.
 *
 * @param fs file system.
 */
class FSUtil(fs: FileSystem) extends Logging {

  def exists(dirName: String): Boolean = {
    fs.exists(new Path(dirName))
  }

  def getFilePathList(dirName: String): Array[Path] = {
    val fileStatus = fs.listStatus(new Path(dirName))
    FileUtil.stat2Paths(fileStatus)
  }

  def getStringFromPath(path: Path): String = {
    def readLines = fromInputStream(fs.open(path))
    readLines.takeWhile(_ != null).mkString
  }

  def writeAsFile(filePath: String, contents: String): Unit = {
    try {
      val outStream = fs.create(new Path(filePath))
      outStream.writeBytes(contents)
      outStream.close()
    } catch {
      case e: IOException =>
        logError(
          "IO Exception while writing the output File" + e.printStackTrace()
        )
    }
  }

  /**
   * Copy the file from source to destination path.
   *
   * @param source      path to the source file.
   * @param destination path to the destination for copy.
   */
  def copyDir(source: String, destination: String): Unit = {
    try {
      FileUtil.copy(
        FileSystem.get(new URI(source), new Configuration()),
        new Path(source),
        FileSystem.get(new URI(destination), new Configuration()),
        new Path(destination),
        false,
        new Configuration()
      )
    } catch {
      case e: IOException =>
        logError(
          s"IO Exception while copying the file from source path $source," +
            s" to destination  $destination" + e.printStackTrace()
        )
    }
  }

  def mkdir(path: String): Unit = {
    if (fs.exists(new Path(path))) throw new IOException("Directory already exists")
    fs.mkdirs(new Path(path))
  }

  def delete(path: String): Unit = {
    fs.delete(new Path(path), true)
  }

}

/**
 * Companion object for FS Util.
 */
object FSUtil {
  def apply(fsURI: String): FSUtil =
    new FSUtil(FileSystem.get(new URI(fsURI), new Configuration()))

  def apply(fsURI: String, storageSecretKey: String): FSUtil = {
    val uri = new URI(fsURI)
    val conf = new Configuration()
    conf.set(s"fs.azure.account.key.${uri.getHost}", storageSecretKey)
    conf.set("fs.azure.account.auth.type", "SharedKey")
    new FSUtil(FileSystem.get(uri, conf))
  }
}
