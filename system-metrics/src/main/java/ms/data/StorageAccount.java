package ms.data;

import org.hibernate.validator.constraints.NotEmpty;

/**
 * Pojo class contains the details like the uri of the remote storage location and secret key to access it.
 */
public class StorageAccount {

    @NotEmpty
    private String fsuri;

    private String secretKey;

    public String getFsuri() {
        return fsuri;
    }

    public void setfsuri(String fsuri) {
        this.fsuri = fsuri;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public String toString() {
        return "{ fsURI: \"" + fsuri + "\"" + "\n"
                + "secretKey: \"" + secretKey + "\" }";
    }
}
