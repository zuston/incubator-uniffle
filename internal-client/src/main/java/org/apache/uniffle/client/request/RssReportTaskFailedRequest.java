package org.apache.uniffle.client.request;

public class RssReportTaskFailedRequest {
  private String appId;
  private int shuffleId;
  private String taskId;
  private long taskAttemptId;
  private String exceptionMsg;

  public RssReportTaskFailedRequest(String appId, int shuffleId, String taskId, long taskAttemptId,
      String exceptionMsg) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskAttemptId = taskAttemptId;
    this.taskId = taskId;
    this.exceptionMsg = exceptionMsg;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public String getTaskId() {
    return taskId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public String getExceptionMsg() {
    return exceptionMsg;
  }
}
