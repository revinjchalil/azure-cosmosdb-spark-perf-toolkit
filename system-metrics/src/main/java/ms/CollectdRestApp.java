package ms;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import ms.resources.CollectdResource;

/**
 * Main Dropwizard application.
 */
public class CollectdRestApp extends Application<CollectdRestConfig> {
  public static void main(String[] args) throws Exception {
    new CollectdRestApp().run(args);
  }

  @Override
  public void run(CollectdRestConfig collectdRestConfig, Environment environment) {
    final CollectdResource collectdResource = new CollectdResource();
    environment.jersey().register(collectdResource);
  }

  @Override
  public void initialize(Bootstrap<CollectdRestConfig> bootstrap) {

  }
}