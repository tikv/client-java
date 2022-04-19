/*
 * Copyright 2022 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration.ApiVersion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb.Store;

public class StoreConfig {
  private static final Logger logger = LoggerFactory.getLogger(StoreConfig.class);

  private static JsonObject getConfig(PDClient client) {
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(BackOffer.PD_INFO_BACKOFF);
    List<Store> stores = client.getAllStores(backOffer);
    if (stores.isEmpty()) {
      throw new IllegalStateException("No store found");
    }

    Store store = stores.get(0);
    String statusAddr = store.getStatusAddress();
    String api = "http://" + statusAddr + "/config";
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(api);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        HttpEntity entity = response.getEntity();
        String content = EntityUtils.toString(entity);
        return new Gson().fromJson(content, JsonObject.class);
      }
    } catch (Exception e) {
      logger.error("Failed to get store api version: ", e);
      throw new IllegalStateException(e);
    }
  }

  public static ApiVersion acquireApiVersion(PDClient client) {
    return getConfig(client).get("storage").getAsJsonObject().get("api-version").getAsInt() == 1
        ? ApiVersion.V1
        : ApiVersion.V2;
  }

  public static boolean ifTllEnable(PDClient client) {
    return getConfig(client).get("storage").getAsJsonObject().get("enable-ttl").getAsBoolean();
  }
}
