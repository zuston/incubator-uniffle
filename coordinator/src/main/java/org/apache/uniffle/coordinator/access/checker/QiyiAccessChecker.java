package org.apache.uniffle.coordinator.access.checker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.filesystem.HadoopFilesystemProvider;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.coordinator.AccessManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.access.AccessCheckResult;
import org.apache.uniffle.coordinator.access.AccessInfo;
import org.apache.uniffle.coordinator.metric.CoordinatorMetrics;

import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_GREYSCALE_MODE_ENABLE;
import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_QIYI_ACCESS_BLACK_LIST_PATH;
import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_QIYI_ACCESS_WHITE_LIST_PATH;
import static org.apache.uniffle.coordinator.CoordinatorConf.COORDINATOR_SAFE_MODE_ENABLE;

/**
 * @author zhangjunfan
 * @date 2022/8/4
 */
public class QiyiAccessChecker extends AbstractAccessChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(QiyiAccessChecker.class);

  /**
   * If entering safe mode, it will reject all access request.
   */
  private boolean safemode = false;

  private boolean inGreyScaleMode = false;

  // The black list is only valid when grey scale mode disable
  private volatile List<Factor> blackListInfo;
  private volatile long blackListFileLastModifiedTime;
  private ScheduledExecutorService updateBlacklistScheduler;

  // The white list is only valid when grey scale mode enable
  private volatile List<Factor> whiteListInfo;
  private volatile long whiteListFileLastModifiedTime;
  private ScheduledExecutorService updateWhitelistScheduler;

  public QiyiAccessChecker(AccessManager accessManager) throws Exception {
    super(accessManager);

    CoordinatorConf coordinatorConf = accessManager.getCoordinatorConf();

    this.safemode = coordinatorConf.get(COORDINATOR_SAFE_MODE_ENABLE);
    if (safemode) {
      LOGGER.info("Currently uniffle enters safe-mode, it will reject all access requests.");
      return;
    }

    this.inGreyScaleMode = coordinatorConf.get(COORDINATOR_GREYSCALE_MODE_ENABLE);

    if (!inGreyScaleMode) {
      String blackListPath = coordinatorConf.get(COORDINATOR_QIYI_ACCESS_BLACK_LIST_PATH);
      if (StringUtils.isNotEmpty(blackListPath)) {
        loadAndRefreshBlackList(blackListPath, accessManager.getHadoopConf());
      }
      return;
    }

    LOGGER.info("Currently uniffle enters greyscale-mode, only jobs in whitelist will be accepted.");
    String confPath = coordinatorConf.get(COORDINATOR_QIYI_ACCESS_WHITE_LIST_PATH);
    if (StringUtils.isEmpty(confPath)) {
      throw new RssException("Once enable greyscale mode, the conf of " + COORDINATOR_QIYI_ACCESS_WHITE_LIST_PATH
          + " should be set");
    }
    loadAndRefreshWhiteList(confPath, accessManager.getHadoopConf());
  }

  private void loadAndRefreshBlackList(String blackListPath, Configuration hadoopConf) throws Exception {
    Path blackListFilePath = new Path(blackListPath);
    FileSystem fileSystem = HadoopFilesystemProvider.getFilesystem(blackListFilePath, hadoopConf);
    FileStatus fileStatus = fileSystem.getFileStatus(blackListFilePath);
    if (!fileStatus.isFile()) {
      throw new RssException("There is no such black list file path of " + blackListFilePath);
    }
    this.blackListFileLastModifiedTime = fileStatus.getModificationTime();
    this.blackListInfo = loadFactorsFromFile(fileSystem, blackListFilePath);
    if (blackListInfo == null) {
      throw new RssException("Failed to load black list when initializing");
    }
    LOGGER.info("Initialized black list info: {}", blackListInfo);

    updateBlacklistScheduler = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("Update-black-list-%d")
    );

    updateBlacklistScheduler.scheduleAtFixedRate(
        () -> {
          try {
            FileStatus status = fileSystem.getFileStatus(blackListFilePath);
            if (status.getModificationTime() != blackListFileLastModifiedTime) {
              List<Factor> factors = loadFactorsFromFile(fileSystem, blackListFilePath);
              if (factors != null) {
                this.blackListInfo = factors;
                LOGGER.info("Update black list info: {}", blackListInfo);
              }
              this.blackListFileLastModifiedTime = status.getModificationTime();
            }
          } catch (Exception e) {
            // ignore
          }
        },
        60,
        60,
        TimeUnit.SECONDS
    );
  }

  private void loadAndRefreshWhiteList(String confPath, Configuration hadoopConf) throws Exception {
    Path whiteListPath = new Path(confPath);
    FileSystem fileSystem = HadoopFilesystemProvider.getFilesystem(whiteListPath, hadoopConf);
    FileStatus fileStatus = fileSystem.getFileStatus(whiteListPath);
    if (!fileStatus.isFile()) {
      throw new RssException("There is no such white list file path of " + confPath);
    }
    this.whiteListFileLastModifiedTime = fileStatus.getModificationTime();
    this.whiteListInfo = loadFactorsFromFile(fileSystem, whiteListPath);
    if (whiteListInfo == null) {
      throw new RssException("Failed to load white list when initializing");
    }
    LOGGER.info("Initialized white list info: {}", whiteListInfo);

    updateWhitelistScheduler = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.getThreadFactory("Update-whitelist")
    );
    updateWhitelistScheduler.scheduleAtFixedRate(
        () -> {
          try {
            FileStatus status = fileSystem.getFileStatus(whiteListPath);
            if (whiteListFileLastModifiedTime != status.getModificationTime()) {
              List<Factor> factors = loadFactorsFromFile(fileSystem, whiteListPath);
              if (factors != null) {
                this.whiteListInfo = factors;
                LOGGER.info("Update white list info: {}", whiteListInfo);
              }
              this.whiteListFileLastModifiedTime = status.getModificationTime();
            }
          } catch (IOException e) {
            // Ignore.
          }
        },
        60,
        60,
        TimeUnit.SECONDS
    );
  }

  /**
   *
   * File content as list:
   * hadoop-user=xx,wf-name=xxxxx,co-name=xxx,app-name=xxx,co-id=xxx
   */
  @VisibleForTesting
  protected List<Factor> loadFactorsFromFile(FileSystem fileSystem, Path confPath) {
    List<Factor> factors = new ArrayList<>();
    try (FSDataInputStream in = fileSystem.open(confPath)) {
      String content = IOUtils.toString(in, StandardCharsets.UTF_8);
      for (String item : content.split(IOUtils.LINE_SEPARATOR_UNIX)) {
        if (StringUtils.isEmpty(item)) {
          continue;
        }
        final Factor factor = new Factor();
        String[] arr = item.trim().split(",");
        for (String pair : arr) {
          pair = pair.trim();
          if (pair.startsWith("hadoop-user")) {
            factor.hadoopUser = pair.split("=", 2)[1].trim();
          }
          if (pair.startsWith("wf-name")) {
            factor.wfName = pair.split("=", 2)[1].trim();
          }
          if (pair.startsWith("co-name")) {
            factor.coName = pair.split("=", 2)[1].trim();
          }
          if (pair.startsWith("app-name")) {
            factor.appName = pair.split("=", 2)[1].trim();
          }
          if (pair.startsWith("co-id")) {
            factor.coId = pair.split("=", 2)[1].trim();
          }
          if (pair.startsWith("platform")) {
            factor.platform = pair.split("=", 2)[1].trim();
          }
        }
        factors.add(factor);
      }
      return factors;
    } catch (Exception e) {
      LOGGER.error("Failed to loading data from {}", confPath, e);
    }
    return null;
  }

  public AccessCheckResult checkInternal(AccessInfo accessInfo) {
    /**
     * Access limitation
     * 1. Once in safe mode, it will reject all access requests.
     * 2. When in grey scale mode, it must be in white list
     * 3. Retried job will fallback.
     */
    if (safemode) {
      LOGGER.warn("Rejected due to in safe mode, app info: {}", accessInfo.getExtraProperties());
      return new AccessCheckResult(false, "Uniffle is in safe mode and all access requests have been rejected.");
    }

    Map<String, String> jobInfos = accessInfo.getExtraProperties();

    JobMeta jobMeta = createJobInfo(accessInfo);

    if (inGreyScaleMode) {
      if (whiteListInfo != null && !isHit(jobMeta, whiteListInfo)) {
        LOGGER.warn("Rejected due to not in whitelist, app info: {}", jobInfos);
        return new AccessCheckResult(false, "App is not in white-list and fallback to ess.");
      }
    } else {
      if (blackListInfo != null && isHit(jobMeta, blackListInfo)) {
        LOGGER.warn("Rejected due to in blacklist, app info: {}", jobInfos);
        return new AccessCheckResult(false, "App is in black-list and fallback to ess.");
      }
    }

    if (jobMeta.isRetry) {
      LOGGER.info("Rejected due to job retried limitation , app info: {}", jobInfos);
      return new AccessCheckResult(false, "It's a retry job and fallback to ess.");
    }

    LOGGER.info("Passed app info: {}", jobInfos);
    return new AccessCheckResult(true, Constants.COMMON_SUCCESS_MESSAGE);
  }

  @VisibleForTesting
  protected JobMeta createJobInfo(AccessInfo accessInfo) {
    Map<String, String> jobInfos = accessInfo.getExtraProperties();

    String oozieJobInfos = jobInfos.get("oozie.job.infos");
    String pilotJobInfos = jobInfos.get("pilot.job.infos");

    String wfName = null;
    String coName = null;
    String coId = null;
    boolean isRetry = true;
    String platform = null;
    if (StringUtils.isNotEmpty(oozieJobInfos)) {
      platform = "gear";

      String wfNameVal = Arrays.stream(oozieJobInfos.split(","))
          .filter(x -> x.startsWith("wf.name"))
          .findFirst()
          .orElse(StringUtils.EMPTY);
      if (StringUtils.isNotEmpty(wfNameVal)) {
        wfName = wfNameVal.split("=")[1];
      }

      String coNameVal = Arrays.stream(oozieJobInfos.split(","))
          .filter(x -> x.startsWith("coord.name"))
          .findFirst()
          .orElse(StringUtils.EMPTY);
      if (StringUtils.isNotEmpty(coNameVal)) {
        coName = coNameVal.split("=")[1];
      }

      String coIdVal = Arrays.stream(oozieJobInfos.split(","))
          .filter(x -> x.startsWith("coord.id"))
          .findFirst()
          .orElse(StringUtils.EMPTY);
      if (StringUtils.isNotEmpty(coIdVal)) {
        coId = coIdVal.split("=")[1].split("@")[0];
      }

      List<String> infoPairs = Arrays.asList(oozieJobInfos.split(","));
      for (String infoPair : infoPairs) {
        if (infoPair.startsWith("isretry=")) {
          isRetry = Boolean.parseBoolean(infoPair.split("=", 2)[1]);
        }
      }
    } else if (StringUtils.isNotEmpty(pilotJobInfos)) {
      platform = "pilot";
      /**
       * {
       *     "spark.pilot.gear.info":{
       *         "gearCoordinatorId":"8934195-220906104926298-oozie-oozi-C",
       *         "gearWorkflowName":"wf_1"
       *     },
       *     "spark.pilot.planId":"xxxx"
       * }
       */
      Map<String, Object> pilotMetaInfos = new Gson().fromJson(pilotJobInfos, Map.class);
      if (pilotMetaInfos.containsKey("spark.pilot.gear.info")) {
        Object jsonVal = pilotMetaInfos.get("spark.pilot.gear.info");
        Map<String, String> kvs = new Gson().fromJson(jsonVal.toString(), Map.class);
        wfName = kvs.get("gearWorkflowName");
        coName = kvs.get("gearCoordinatorName");
        String gearCoIdString = kvs.get("gearCoordinatorId");
        if (StringUtils.isNotEmpty(gearCoIdString)) {
          coId = gearCoIdString.split("@", 2)[0];
        }

        String rawIsRetry = kvs.getOrDefault("gear.workflow.action.isRetry", "false");
        try {
          isRetry = Boolean.valueOf(rawIsRetry);
        } catch (Exception e) {
          LOGGER.error("Converting isRetry.", e);
          isRetry = false;
        }
      } else {
        // To be compatible with older version of Pilot.
        /**
         * {
         *     "spark.gear.cluster.name":"xxx",
         *     "spark.gear.workflow.id":"xxxx",
         *     "spark.gear.workflow.name":"xxxx"
         * }
         */
        isRetry = false;
        wfName = (String) pilotMetaInfos.get("spark.gear.workflow.name");
      }
    }

    String appName = jobInfos.get("app.name");
    String user = jobInfos.get("user");

    return new JobMeta(coName, wfName, coId, appName, user, isRetry, platform);
  }

  @Override
  public AccessCheckResult check(AccessInfo accessInfo) {
    try {
      AccessCheckResult accessCheckResult = checkInternal(accessInfo);
      if (!accessCheckResult.isSuccess()) {
        CoordinatorMetrics.counterTotalQiyiAccessCheckerDeniedRequest.inc();
      }
      return accessCheckResult;
    } catch (Exception e) {
      LOGGER.error("It should not happen. Access info: {}", accessInfo, e);
      CoordinatorMetrics.counterTotalQiyiAccessCheckerDeniedRequest.inc();
      return new AccessCheckResult(false, "Unexpected exception happened in coordinator side.");
    }
  }

  protected boolean isHit(JobMeta jobInfo, List<Factor> factors) {
    if (factors == null || factors.isEmpty()) {
      return false;
    }

    if (jobInfo == null) {
      return false;
    }

    for (Factor factor : factors) {
      if (factor.hadoopUser == null && factor.appName == null && factor.coName == null
          && factor.wfName == null && factor.coId == null && factor.platform == null) {
        continue;
      }
      boolean userHit = factor.hadoopUser != null ? factor.hadoopUser.equals(jobInfo.user) : true;
      if (!userHit) {
        continue;
      }
      boolean appNameHit = factor.appName != null ? factor.appName.equals(jobInfo.appName) : true;
      if (!appNameHit) {
        continue;
      }
      // Support pattern match
      boolean wfNameHit = factor.wfName != null ? equalOrWildcardMatch(factor.wfName, jobInfo.wfName) : true;
      if (!wfNameHit) {
        continue;
      }
      boolean coNameHit = factor.coName != null ? equalOrWildcardMatch(factor.coName, jobInfo.coName) : true;
      if (!coNameHit) {
        continue;
      }
      boolean coIdHit = factor.coId != null ? factor.coId.equals(jobInfo.coId) : true;
      if (!coIdHit) {
        continue;
      }
      boolean platformHit = factor.platform != null ? factor.platform.equals(jobInfo.platform) : true;
      if (!platformHit) {
        continue;
      }
      return true;
    }

    return false;
  }

  private boolean equalOrWildcardMatch(String expected, String actual) {
    if (StringUtils.isEmpty(actual)) {
      return false;
    }

    if (expected.contains("*")) {
      int idx = expected.indexOf("*");
      String prefix = expected.substring(0, idx);
      if (idx == expected.length() - 1) {
        return actual.startsWith(prefix);
      }

      String suffix = expected.substring(idx + 1);
      return actual.startsWith(prefix) && actual.endsWith(suffix);
    } else {
      return expected.equals(actual);
    }
  }

  static class JobMeta {
    private String coName;
    private String wfName;
    private String coId;
    private String appName;
    private String user;
    private boolean isRetry;
    // origin platform.
    private String platform;

    JobMeta() {
    }

    JobMeta(String coName, String wfName, String coId, String appName, String user, boolean isRetry, String platform) {
      this.coName = coName;
      this.wfName = wfName;
      this.coId = coId;
      this.appName = appName;
      this.user = user;
      this.isRetry = isRetry;
      this.platform = platform;
    }
  }

  @Override
  public void close() throws IOException {
    if (updateBlacklistScheduler != null) {
      updateBlacklistScheduler.shutdown();
    }
    if (updateWhitelistScheduler != null) {
      updateWhitelistScheduler.shutdown();
    }
  }

  static class Factor {
    private String hadoopUser;
    private String wfName;
    private String coName;
    private String coId;
    private String appName;
    // Like gear/pilot
    private String platform;

    public void setHadoopUser(String hadoopUser) {
      this.hadoopUser = hadoopUser;
    }

    public void setWfName(String wfName) {
      this.wfName = wfName;
    }

    public void setCoName(String coName) {
      this.coName = coName;
    }

    public void setCoId(String coId) {
      this.coId = coId;
    }

    public void setAppName(String appName) {
      this.appName = appName;
    }

    public void setPlatform(String platform) {
      this.platform = platform;
    }

    @Override
    public String toString() {
      return "Factor{"
          + "hadoopUser='" + hadoopUser + '\''
          + ", wfName='" + wfName + '\''
          + ", coName='" + coName + '\''
          + ", coId='" + coId + '\''
          + ", appName='" + appName + '\''
          + ", platform='" + platform + '\''
          + '}';
    }
  }
}
