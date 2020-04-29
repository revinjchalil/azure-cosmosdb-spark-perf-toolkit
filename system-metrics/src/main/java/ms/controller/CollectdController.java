package ms.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import ms.Utils.FSUtil;
import ms.data.Constants;
import ms.data.StorageAccount;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Controller class for executing the commands.
 */
public class CollectdController {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectdController.class);

  /**
   * Starts collectd agent on the host.
   * @return output of the command
   * @throws Exception
   */
  public static String startCollectdAgent() throws Exception {
    LOGGER.info("starting collectd agent");
    List<String> command = new ArrayList<String>(Arrays.asList(
            "systemctl", "start", "collectd"));
    String commandOutput = executeCommand(command);
    LOGGER.info("started collectd agent");
    return commandOutput;
  }

  /**
   *stops the collectd agent on the host
   * @return  output of the command
   * @throws Exception
   */
  public static String stopCollectdAgent() throws Exception {
    LOGGER.info("stopping collectd agent");
    List<String> command = new ArrayList<String>(Arrays.asList(
            "systemctl", "stop", "collectd"));
    String commandOutput = executeCommand(command);
    LOGGER.info("stopped collectd agent");
    return commandOutput;
  }

  /**
   * renames stat files by removing date component, example renames percent-cpu-2020-02-11.csv
   *  to percent-cpu.csv
   * @return  output of the command
   * @throws Exception
   */
  public static String renameStatFiles() throws Exception {
    LOGGER.info("renaming stat files");
    List<String> command = new ArrayList<String>(Arrays.asList(
            "/bin/sh", "-c", "find " + Constants.COLLECTD_STORAGE_DIR + " -type f -name \"*\""
                    + " -exec rename 's/-\\d{4}-\\d{2}-\\d{2}$//' {} +"));
    String commandOutput = executeCommand(command);
    return commandOutput;
  }

  /**
   * renames directories like cpu-0 to metricType=cpu-0
   *  for spark to easily read the files
   * @return  output of the command
   * @throws Exception
   */
  public static void renameMetricsDirectory() throws Exception {
    LOGGER.info("renaming cpu directory to metricType=cpu");
    removeDateFromDirectory("cpu");

    LOGGER.info("renaming disk directory to metricType=disk");
    removeDateFromDirectory("disk");

    LOGGER.info("renaming memory directory to metricType=memory");
    removeDateFromDirectory("memory");

    LOGGER.info("renaming interface directory to metricType=interface");
    removeDateFromDirectory("interface");
  }

  private static String removeDateFromDirectory(String filePrefix) throws Exception {
    List<String> renameDirectory = new ArrayList<>(Arrays.asList(
            "/bin/sh", "-c", "find " + Constants.COLLECTD_STORAGE_DIR + " -type d -name \"" +
                    filePrefix + "*\""
                    + " -exec sh -c 'mv {} $(dirname {})/metricComponent=$(basename {})' \\;"));
    String commandOutput = executeCommand(renameDirectory);
    return commandOutput;
  }

  /**
   * deletes collectd stats directory
   * @return output of the command
   * @throws Exception
   */
  public static String deleteCollectdStatsDirectory() throws Exception {
    LOGGER.info("deleting older collectd metrics directory");
    List<String> command = new ArrayList<>(Arrays.asList(
            "rm", "-rf", Constants.COLLECTD_STORAGE_DIR));
    String commandOutput = executeCommand(command);
    LOGGER.info("deleted older collectd metrics directory");
    return commandOutput;
  }

  /**
   *
   * @return fully qualified hostname on which the server is running
   * @throws Exception
   */
  public static String getFullyQualifiedHostname() throws Exception {
    List<String> command = new ArrayList<>(Arrays.asList(
            "hostname", "-A"));
    String hostname = executeCommand(command);
    hostname = hostname.replace("\n","").trim();
    return hostname;
  }

  /**
   * executes any command given as a list of strings.
   * @param command
   * @return output of the command
   * @throws Exception
   */
  private static String executeCommand(List<String> command) throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command(command);
    String commandOutput;
    try {
      Process process = processBuilder.start();
      StringBuilder output = new StringBuilder();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
              process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
      }
      process.waitFor();
      commandOutput = output.toString();
      LOGGER.info("command output is {}", commandOutput);
    } catch (IOException | InterruptedException ie) {
      LOGGER.warn("Exception while executing the command {}", ie.getMessage());
      throw new Exception(ie);
    }
    return commandOutput;
  }

  /**
   * uploads all the metrics to the storage location
   * @param storageAccount
   * @throws URISyntaxException
   * @throws IOException
   */
  public static void uploadSystemMetricsFileToStorage(StorageAccount storageAccount)
          throws Exception {

    LOGGER.info("remoteStorageURi is " + storageAccount.getFsuri());
    LOGGER.info("secretKey is " + storageAccount.getSecretKey());
    URI uri = new URI(storageAccount.getFsuri());
    Configuration configuration = new Configuration();
    if (!StringUtils.isEmpty(storageAccount.getSecretKey()) && !storageAccount.getSecretKey().equals("null")) {
      configuration.set("fs.azure.account.key." + uri.getHost(), storageAccount.getSecretKey());
      configuration.set("fs.azure.account.auth.type", "SharedKey");
    }
    FileSystem remoteFs = FileSystem.get(uri, configuration);

    String hostname = getFullyQualifiedHostname();
    LOGGER.info("hostname is {}", hostname);
    String collectdStatsDirectory = Constants.COLLECTD_STORAGE_URI + "/" + hostname;
    URI collectdStatsDirectoryUri = new URI(collectdStatsDirectory);
    LOGGER.info("localStorageURI is " + collectdStatsDirectoryUri);
    FileSystem localFs = FileSystem.get(collectdStatsDirectoryUri, new Configuration());

    FSUtil.copyLocalStorageToRemoteStorage(localFs, remoteFs, collectdStatsDirectory,
            storageAccount.getFsuri());
  }
}