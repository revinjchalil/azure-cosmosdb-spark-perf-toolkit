package ms.resources;

import com.codahale.metrics.annotation.Timed;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import ms.controller.CollectdController;
import ms.data.StorageAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collectd resource.
 */
@Path("/collectd")
public class CollectdResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectdResource.class);

    /**
     * Starts the collectd service on the host.
     * It first stops a running collectd, deletes the metrics directory and then starts the agent.
     * @return
     * @throws Exception
     */
    @POST
    @Timed
    @Path("/start")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes({MediaType.APPLICATION_JSON})
    public String startCollectdAgent() throws Exception {
        LOGGER.info("received a request to start collectd agent");
        CollectdController.stopCollectdAgent();
        try {
            CollectdController.deleteCollectdStatsDirectory();
        } catch (Exception e) {
            LOGGER.warn("caught an exception while deleting the directory");
        }
        return CollectdController.startCollectdAgent();
    }

    /**
     * Stops the collectd service and uploads the system metrics to the storage location mentioned in storageDetails.
     * @param storageAccount contains filesystem uri and secret key
     * @return
     * @throws Exception
     */
    @POST
    @Timed
    @Path("/stop")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes({MediaType.APPLICATION_JSON})
    public void stopCollectdAgent(@Valid StorageAccount storageAccount)
            throws Exception {
        LOGGER.info("received a request to stop collectd agent and upload the "
                + "results to remote storage {}", storageAccount);
        CollectdController.stopCollectdAgent();
        CollectdController.renameStatFiles();
        CollectdController.renameMetricsDirectory();
        LOGGER.info("uploading metrics to file storage");
        CollectdController.uploadSystemMetricsFileToStorage(storageAccount);
        LOGGER.info("uploaded metrics to file storage");
    }
}