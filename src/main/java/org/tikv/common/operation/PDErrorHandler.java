/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.operation;

import java.util.function.Function;
import org.apache.log4j.Logger;
import org.tikv.common.PDClient;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Pdpb;

public class PDErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = Logger.getLogger(PDErrorHandler.class);
  private final Function<RespT, Pdpb.Error> getError;
  private final PDClient client;

  public PDErrorHandler(Function<RespT, Pdpb.Error> errorExtractor, PDClient client) {
    this.getError = errorExtractor;
    this.client = client;
  }

  @Override
  public boolean handleResponseError(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      return false;
    }
    Pdpb.Error error = getError.apply(resp);
    if (error != null) {
      if (error.getMessage().equalsIgnoreCase("RegionId is 0")) {
        logger.info("region id is 0");
      } else {
        client.updateLeader();
      }
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoPDRPC, new GrpcException(error.toString()));
      return true;
    }
    return false;
  }

  @Override
  public boolean handleRequestError(BackOffer backOffer, Exception e) {
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoPDRPC, e);
    return true;
  }
}
