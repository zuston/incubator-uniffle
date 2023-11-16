package org.apache.uniffle.client.response;

import org.apache.uniffle.common.rpc.StatusCode;

public class RssReportTaskFailedResponse extends ClientResponse {
  public RssReportTaskFailedResponse(StatusCode statusCode) {
    super(statusCode);
  }
}
