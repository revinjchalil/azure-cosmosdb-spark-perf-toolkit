package ms;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Config file for Dropwizard application.
 */
public class CollectdRestConfig extends Configuration {
  @NotEmpty
  private String version;

  @JsonProperty
  public String getVersion() {
    return version;
  }

  @JsonProperty
  public void setVersion(String version) {
    this.version = version;
  }
}