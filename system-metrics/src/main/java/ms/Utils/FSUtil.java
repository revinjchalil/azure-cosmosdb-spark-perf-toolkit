package ms.Utils;

import java.io.IOException;
import ms.data.Constants;
import ms.resources.CollectdResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectdResource.class);

    /**
     * copies a local filesystem to destination filesystem.
     *
     * @param localFileSystem
     * @param destinationFileSystem
     * @param source
     * @param destination
     * @throws IOException
     */
    public static void copyLocalStorageToRemoteStorage(FileSystem localFileSystem,
            FileSystem destinationFileSystem,
            String source, String destination)
            throws IOException {
        copyLocalStorageToRemoteStorageWithRetries(localFileSystem, destinationFileSystem,
                source, destination, 0);
    }

    private static void copyLocalStorageToRemoteStorageWithRetries(FileSystem localFileSystem,
            FileSystem destinationFileSystem,
            String source, String destination,
            int retries)
            throws IOException {
        try {
            FileUtil.copy(localFileSystem,
                    new Path(source),
                    destinationFileSystem,
                    new Path(destination),
                    true,
                    new Configuration());
        } catch (IOException ie) {
            LOGGER.error("Exception while copying files from local to remote "
                    + "location due to: {}", ie.getMessage());
            if (retries < Constants.NUMBER_OF_RETRIES) {
                copyLocalStorageToRemoteStorageWithRetries(localFileSystem, destinationFileSystem,
                        source, destination, retries + 1);
            } else {
                throw new IOException("Exception while copying data from "
                        + "local to remote file system");
            }
        }
    }

    /**
     * creates a path if it doesn't exist in the filesystem
     * @param path path in the remote filesystem
     * @param fs
     * @throws IOException
     */
    public static void mkdir(String path, FileSystem fs) throws IOException {
        if (!fs.exists(new Path(path))) {
            fs.mkdirs(new Path(path));
        }
    }

    /**
     * deletes the given path in the filesystem
     * @param path
     * @param fs
     * @throws IOException
     */
    public static void delete(String path, FileSystem fs) throws IOException {
        fs.delete(new Path(path), true);
    }
}
