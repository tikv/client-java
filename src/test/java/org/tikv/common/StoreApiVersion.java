package org.tikv.common;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb.Store;

public class StoreApiVersion {
  private static final Logger logger = LoggerFactory.getLogger(StoreApiVersion.class);
  private final int version;

  private StoreApiVersion(int version) {
    this.version = version;
  }

  public int get() {
    return version;
  }

  public static StoreApiVersion acquire(PDClient client) {
    int version = 1;
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF);
    for (Store store : client.getAllStores(backOffer)) {
      String statusAddr = store.getStatusAddress();
      String api = "http://" + statusAddr + "/config";
      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
        HttpGet request = new HttpGet(api);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
          HttpEntity entity = response.getEntity();
          String content = EntityUtils.toString(entity);
          JsonObject object = new Gson().fromJson(content, JsonObject.class);
          try {
            version = object.get("storage").getAsJsonObject().get("api-version").getAsInt();
          } catch (Exception ignored) {
          }
        }
      } catch (Exception e) {
        logger.error("Failed to get store api version: ", e);
      }
    }
    return new StoreApiVersion(version);
  }
}
