package org.apache.spark.shuffle;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.client.request.RssFetchClientConfRequest;
import org.apache.uniffle.client.response.RssFetchClientConfResponse;
import org.apache.uniffle.common.ClientType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.request.RssAccessClusterRequest;
import org.apache.uniffle.client.response.RssAccessClusterResponse;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.Constants;

import static org.apache.uniffle.common.util.Constants.ACCESS_INFO_REQUIRED_SHUFFLE_NODES_NUM;

/**
 * @author zhangjunfan
 * @date 2022/8/4
 */
public class QiyiRssShuffleManager implements ShuffleManager {
  private static final Logger LOG = LoggerFactory.getLogger(QiyiRssShuffleManager.class);

  private final ShuffleManager delegate;
  private final List<CoordinatorClient> coordinatorClients;
  private final int accessTimeoutMs;
  private final SparkConf sparkConf;

  public QiyiRssShuffleManager(SparkConf sparkConf, boolean isDriver) throws Exception {
    this.sparkConf = sparkConf;
    accessTimeoutMs = sparkConf.get(RssSparkConfig.RSS_ACCESS_TIMEOUT_MS);
    if (isDriver) {
      coordinatorClients = RssSparkShuffleUtils.createCoordinatorClients(sparkConf);
      delegate = createShuffleManagerInDriver();
    } else {
      coordinatorClients = Collections.emptyList();
      delegate = createShuffleManagerInExecutor();
    }

    if (delegate == null) {
      throw new RssException("Fail to create shuffle manager!");
    }
  }

  public void fetchAndApplyDynamicConf(SparkConf sparkConf) {
    int timeoutMs =
        sparkConf.getInt(
            RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.key(),
            RssSparkConfig.RSS_ACCESS_TIMEOUT_MS.defaultValue().get());
    String user = "EMPTY";
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (Exception e) {
      LOG.error("Errors on getting current user.", e);
    }
    Map<String, String> extraProperties = getAppInfo();
    RssFetchClientConfRequest request =
        new RssFetchClientConfRequest(timeoutMs, user, extraProperties);
    for (CoordinatorClient client : coordinatorClients) {
      RssFetchClientConfResponse response = client.fetchClientConf(request);
      if (response.getStatusCode() == StatusCode.SUCCESS) {
        LOG.info("Success to get conf from {}", client.getDesc());
        RssSparkShuffleUtils.applyDynamicClientConf(sparkConf, response.getClientConf());
        break;
      } else {
        LOG.warn("Fail to get conf from {}", client.getDesc());
      }
    }
  }

  private ShuffleManager createShuffleManagerInDriver() throws RssException {
    ShuffleManager shuffleManager;

    boolean canAccess = tryAccessCluster();
    if (canAccess) {
      try {
        shuffleManager = new RssShuffleManager(sparkConf, true, sparkConf -> fetchAndApplyDynamicConf(sparkConf));
        sparkConf.set(RssSparkConfig.RSS_ENABLED.key(), "true");
        sparkConf.set("spark.shuffle.manager", RssShuffleManager.class.getCanonicalName());
        LOG.info("Use RssShuffleManager of Uniffle");
        return shuffleManager;
      } catch (Exception exception) {
        LOG.warn("Fail to create RssShuffleManager, fallback to SortShuffleManager {}", exception.getMessage());
      }
    }

    try {
      shuffleManager = RssSparkShuffleUtils.loadShuffleManager(Constants.SORT_SHUFFLE_MANAGER_NAME, sparkConf, true);
      sparkConf.set(RssSparkConfig.RSS_ENABLED.key(), "false");
      sparkConf.set("spark.shuffle.manager", "sort");
      LOG.info("Use SortShuffleManager of ExternalShuffleService");
    } catch (Exception e) {
      throw new RssException(e.getMessage());
    }

    return shuffleManager;
  }

  private boolean internalAccessCluster() {
    String accessId = sparkConf.get(RssSparkConfig.RSS_ACCESS_ID.key(), "uniffle-access-id-empty").trim();
    if (StringUtils.isEmpty(accessId)) {
      LOG.warn("Access id key is empty");
      return false;
    }

    Set<String> assignmentTags = RssSparkShuffleUtils.getAssignmentTags(sparkConf);

    Map<String, String> extraProperties = getAppInfo();

    String user = "EMPTY";
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (Exception e) {
      LOG.error("Errors on getting user.", e);
    }

    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      try {
        RssAccessClusterResponse response =
            coordinatorClient.accessCluster(new RssAccessClusterRequest(
                accessId, assignmentTags, accessTimeoutMs, extraProperties, user));
        if (response.getStatusCode() == StatusCode.SUCCESS) {
          LOG.warn("Success to access cluster {} using {}", coordinatorClient.getDesc(), accessId);
          return true;
        } else if (response.getStatusCode() == StatusCode.ACCESS_DENIED) {
          LOG.warn("Request to access cluster {} is denied using {} for {}",
              coordinatorClient.getDesc(), accessId, response.getMessage());
          return false;
        } else {
          LOG.warn("Fail to reach cluster {} for {}", coordinatorClient.getDesc(), response.getMessage());
        }
      } catch (Exception e) {
        LOG.warn("Fail to access cluster {} using {} for {}",
            coordinatorClient.getDesc(), accessId, e.getMessage());
      }
    }

    return false;
  }

  private boolean tryAccessCluster() {
    try {
      return internalAccessCluster();
    } catch (Exception e) {
      LOG.error("Errors on accessing cluster", e);
      return false;
    }
  }

  private Map<String, String> getAppInfo() {
    Map<String, String> infos = new HashMap<>();

    infos.put(
        ACCESS_INFO_REQUIRED_SHUFFLE_NODES_NUM,
        String.valueOf(RssSparkShuffleUtils.getRequiredShuffleServerNumber(sparkConf))
    );

    String appName = sparkConf.get("spark.app.name", StringUtils.EMPTY);
    infos.put("app.name", appName);
    // When initializing shuffle manager, the spark.app.id is not in sparkConf, It will throw noSuchElementException.
    // This should be improved in Spark.
    // infos.put("app.id", sparkConf.getAppId());

    String oozieJobInfo = sparkConf.get("spark.oozie.job.info", StringUtils.EMPTY);
    infos.put("oozie.job.infos", oozieJobInfo);

    Map<String, String> pilotInfos = new HashMap<>();
    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      if (tuple._1.startsWith("spark.pilot") || tuple._1.startsWith("spark.gear")) {
        pilotInfos.put(tuple._1, tuple._2);
      }
    }
    infos.put("pilot.job.infos", new Gson().toJson(pilotInfos));

    try {
      String user = sparkConf.get("spark.test.user", StringUtils.EMPTY);
      if (StringUtils.isEmpty(user)) {
        user = UserGroupInformation.getCurrentUser().getShortUserName();
      }
      infos.put("user", user);
    } catch (IOException e) {
      LOG.info("Failed to load user", e);
    }

    return infos;
  }

  private ShuffleManager createShuffleManagerInExecutor() throws RssException {
    ShuffleManager shuffleManager;
    // get useRSS from spark conf
    boolean useRSS = sparkConf.get(RssSparkConfig.RSS_ENABLED);
    if (useRSS) {
      // Executor will not do any fallback
      shuffleManager = new RssShuffleManager(sparkConf, false);
      LOG.info("Use RssShuffleManager");
    } else {
      try {
        shuffleManager = RssSparkShuffleUtils.loadShuffleManager(
            Constants.SORT_SHUFFLE_MANAGER_NAME, sparkConf, false);
        LOG.info("Use SortShuffleManager");
      } catch (Exception e) {
        throw new RssException(e.getMessage());
      }
    }
    return shuffleManager;
  }

  public ShuffleManager getDelegate() {
    return delegate;
  }


  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, ShuffleDependency<K, V, C> dependency) {
    return delegate.registerShuffle(shuffleId, dependency);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle,
      long mapId,
      TaskContext context,
      ShuffleWriteMetricsReporter metrics) {
    return delegate.getWriter(handle, mapId, context, metrics);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return delegate.getReader(handle,
        startPartition, endPartition, context, metrics);
  }

  // The interface is only used for compatibility with spark 3.1.2
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    ShuffleReader<K, C> reader = null;
    try {
      reader = (ShuffleReader<K, C>)delegate.getClass().getDeclaredMethod(
          "getReader",
          ShuffleHandle.class,
          int.class,
          int.class,
          int.class,
          int.class,
          TaskContext.class,
          ShuffleReadMetricsReporter.class).invoke(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics);
    } catch (Exception e) {
      throw new RssException(e);
    }
    return reader;
  }

  // The interface is only used for compatibility with spark 3.0.1
  public <K, C> ShuffleReader<K, C> getReaderForRange(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    ShuffleReader<K, C> reader = null;
    try {
      reader = (ShuffleReader<K, C>)delegate.getClass().getDeclaredMethod(
          "getReaderForRange",
          ShuffleHandle.class,
          int.class,
          int.class,
          int.class,
          int.class,
          TaskContext.class,
          ShuffleReadMetricsReporter.class).invoke(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics);
    } catch (Exception e) {
      throw new RssException(e);
    }
    return reader;
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return delegate.unregisterShuffle(shuffleId);
  }

  @Override
  public void stop() {
    delegate.stop();
    coordinatorClients.forEach(CoordinatorClient::close);
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return delegate.shuffleBlockResolver();
  }
}
