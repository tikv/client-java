package org.tikv.grpc;

import io.grpc.*;
import java.util.*;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.log.SlowLog;
import org.tikv.common.log.SlowLogImpl;
import org.tikv.common.log.SlowLogSpan;

public class ClientTracingInterceptor implements ClientInterceptor {

  private static final Logger logger = LoggerFactory.getLogger(ClientTracingInterceptor.class);

  private final OperationNameConstructor operationNameConstructor;
  private final boolean streaming;
  private final boolean verbose;
  private final Set<ClientRequestAttribute> tracedAttributes;

  public ClientTracingInterceptor() {
    this.operationNameConstructor = OperationNameConstructor.DEFAULT;
    this.streaming = false;
    this.verbose = false;
    this.tracedAttributes = new HashSet<>();
  }

  private ClientTracingInterceptor(
      OperationNameConstructor operationNameConstructor,
      boolean streaming,
      boolean verbose,
      Set<ClientRequestAttribute> tracedAttributes) {
    this.operationNameConstructor = operationNameConstructor;
    this.streaming = streaming;
    this.verbose = verbose;
    this.tracedAttributes = tracedAttributes;
  }

  /**
   * Use this intercepter to trace all requests made by this client channel.
   *
   * @param channel to be traced
   * @return intercepted channel
   */
  public Channel intercept(Channel channel) {
    return ClientInterceptors.intercept(channel, this);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    final String operationName = operationNameConstructor.constructOperationName(method);
    SlowLog slowLog = new SlowLogImpl(50, new HashMap<>());
    SlowLogSpan span = slowLog.start("interceptCall: " + operationName);

    for (ClientRequestAttribute attr : this.tracedAttributes) {
      switch (attr) {
        case ALL_CALL_OPTIONS:
          // span.setTag("grpc.call_options", callOptions.toString());
          break;
        case AUTHORITY:
          if (callOptions.getAuthority() == null) {
            // span.setTag("grpc.authority", "null");
          } else {
            // span.setTag("grpc.authority", callOptions.getAuthority());
          }
          break;
        case COMPRESSOR:
          if (callOptions.getCompressor() == null) {
            // span.setTag("grpc.compressor", "null");
          } else {
            // span.setTag("grpc.compressor", callOptions.getCompressor());
          }
          break;
        case DEADLINE:
          if (callOptions.getDeadline() == null) {
            // span.setTag("grpc.deadline_millis", "null");
          } else {
            // span.setTag(
            //    "grpc.deadline_millis",
            //    callOptions.getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
          }
          break;
        case METHOD_NAME:
          // span.setTag("grpc.method_name", method.getFullMethodName());
          break;
        case METHOD_TYPE:
          if (method.getType() == null) {
            // span.setTag("grpc.method_type", "null");
          } else {
            // span.setTag("grpc.method_type", method.getType().toString());
          }
          break;
        case HEADERS:
          break;
      }
    }

    ForwardingClientCall.SimpleForwardingClientCall result =
        new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {

          private void end() {
            slowLog.log();
          }

          @Override
          public void start(Listener<RespT> responseListener, Metadata headers) {
            SlowLogSpan span1 = slowLog.start("Started call");
            Listener<RespT> tracingResponseListener =
                new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                    responseListener) {

                  @Override
                  public void onHeaders(Metadata headers) {
                    SlowLogSpan span =
                        slowLog.start("Response headers received：" + headers.toString());
                    delegate().onHeaders(headers);
                    span.end();
                  }

                  @Override
                  public void onMessage(RespT message) {
                    SlowLogSpan span = slowLog.start("Response received");
                    delegate().onMessage(message);
                    span.end();
                  }

                  @Override
                  public void onClose(Status status, Metadata trailers) {
                    SlowLogSpan span = null;
                    if (status.getCode().value() == 0) {
                      span = slowLog.start("Call closed");
                    } else {
                      span = slowLog.start("Call failed: " + status.getDescription());
                    }
                    delegate().onClose(status, trailers);
                    span.end();
                    end();
                  }
                };
            delegate().start(tracingResponseListener, headers);
            span1.end();
          }

          @Override
          public void cancel(@Nullable String message, @Nullable Throwable cause) {
            String errorMessage;
            if (message == null) {
              errorMessage = "Error";
            } else {
              errorMessage = message;
            }
            SlowLogSpan span = null;
            if (cause == null) {
              span = slowLog.start(errorMessage);
            } else {
              span = slowLog.start(errorMessage + cause.getMessage());
            }
            delegate().cancel(message, cause);
            span.end();
            end();
          }

          @Override
          public void halfClose() {
            SlowLogSpan span = slowLog.start("Finished sending messages");
            delegate().halfClose();
            span.end();
            end();
          }

          @Override
          public void sendMessage(ReqT message) {
            SlowLogSpan span = slowLog.start("Message sent");
            delegate().sendMessage(message);
            span.end();
          }
        };

    span.end();
    return result;
  }

  public enum ClientRequestAttribute {
    METHOD_TYPE,
    METHOD_NAME,
    DEADLINE,
    COMPRESSOR,
    AUTHORITY,
    ALL_CALL_OPTIONS,
    HEADERS
  }
}
