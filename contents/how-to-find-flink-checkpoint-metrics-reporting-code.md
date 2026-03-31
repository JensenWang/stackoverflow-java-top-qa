# 如何找到Flink 1.14版本CheckPoint Metrics上报的代码

## 问题
我需要找到Apache Flink 1.14版本中CheckPoint Metrics上报的相关代码，用于理解检查点指标是如何收集和报告的。

## 回答

### Flink CheckPoint Metrics的主要代码位置

在Apache Flink 1.14版本中，CheckPoint Metrics的上报代码主要分布在以下几个模块中：

#### 1. 核心CheckPoint Metrics类

**CheckpointStatsTracker** - 检查点统计跟踪器
```java
// 位置：flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointStatsTracker.java
public interface CheckpointStatsTracker {
    
    /**
     * 报告已完成的检查点统计信息
     */
    void reportCompletedCheckpoint(CompletedCheckpointStats checkpoint);
    
    /**
     * 报告检查点失败统计信息  
     */
    void reportFailedCheckpoint(FailedCheckpointStats checkpoint);
    
    /**
     * 报告检查点恢复统计信息
     */
    void reportRestoredCheckpoint(RestoredCheckpointStats checkpoint);
}
```

**CheckpointMetrics** - 检查点指标数据结构
```java
// 位置：flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointMetrics.java
public class CheckpointMetrics implements Serializable {
    
    /** 同步部分持续时间（纳秒） */
    private final long syncDurationNanos;
    
    /** 异步部分持续时间（纳秒） */  
    private final long asyncDurationNanos;
    
    /** 检查点大小（字节） */
    private final long bytesBufferedInAlignment;
    
    /** 对齐持续时间（纳秒） */
    private final long alignmentDurationNanos;
    
    // 构造器和getter方法...
}
```

#### 2. Metrics Reporter集成

**CheckpointMetricsReporter** - 检查点指标报告器
```java
// 位置：flink-runtime/src/main/java/org/apache/flink/runtime/metrics/groups/TaskManagerJobMetricGroup.java
public class TaskManagerJobMetricGroup extends ComponentMetricGroup<TaskManagerMetricGroup> {
    
    private final Counter checkpointsCounter;
    private final Histogram checkpointDurationHistogram;
    
    public void reportCheckpointMetrics(CheckpointMetrics metrics) {
        // 更新检查点计数器
        checkpointsCounter.inc();
        
        // 记录检查点持续时间直方图
        checkpointDurationHistogram.update(
            metrics.getSyncDurationNanos() + metrics.getAsyncDurationNanos()
        );
    }
}
```

#### 3. 检查点协调器中的指标上报

**CheckpointCoordinator** - 检查点协调器
```java
// 位置：flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java
public class CheckpointCoordinator {
    
    /** 检查点统计跟踪器 */
    private final CheckpointStatsTracker statsTracker;
    
    /** Metrics组 */
    private final CheckpointCoordinatorMetricGroup metrics;
    
    /**
     * 完成检查点时的指标上报
     */
    private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) {
        // 创建完成的检查点统计信息
        CompletedCheckpointStats completedStats = CompletedCheckpointStats.builder()
            .setCheckpointId(pendingCheckpoint.getCheckpointId())
            .setTriggerTimestamp(pendingCheckpoint.getCheckpointTimestamp())
            .setCompletedTimestamp(System.currentTimeMillis())
            .setTotalBytesPersisted(totalBytes)
            .setEndToEndDuration(endToEndDuration)
            .build();
            
        // 上报给统计跟踪器    
        statsTracker.reportCompletedCheckpoint(completedStats);
        
        // 更新metrics
        metrics.reportCompletedCheckpoint(endToEndDuration);
    }
    
    /**
     * 检查点失败时的指标上报
     */
    private void failPendingCheckpoint(PendingCheckpoint pendingCheckpoint, CheckpointFailureReason reason) {
        FailedCheckpointStats failedStats = new FailedCheckpointStats(
            pendingCheckpoint.getCheckpointId(),
            pendingCheckpoint.getCheckpointTimestamp(),
            reason,
            System.currentTimeMillis() - pendingCheckpoint.getCheckpointTimestamp()
        );
        
        statsTracker.reportFailedCheckpoint(failedStats);
        metrics.reportFailedCheckpoint();
    }
}
```

#### 4. TaskManager端的指标收集

**CheckpointStreamFactory** - 检查点流工厂
```java
// 位置：flink-runtime/src/main/java/org/apache/flink/runtime/state/CheckpointStreamFactory.java
public abstract class CheckpointStreamFactory {
    
    /**
     * 创建检查点状态输出流
     */
    public abstract CheckpointStateOutputStream createCheckpointStateOutputStream(
        CheckpointedStateScope scope) throws IOException;
        
    /**
     * 关闭流并获取状态句柄，同时收集指标
     */
    public static class CheckpointStateOutputStreamImpl implements CheckpointStateOutputStream {
        
        @Override
        public StreamStateHandle closeAndGetHandle() throws IOException {
            // 收集写入的字节数等指标
            long bytesWritten = getBytesWritten();
            
            // 上报给指标系统
            MetricUtils.reportCheckpointBytesWritten(bytesWritten);
            
            return createStateHandle();
        }
    }
}
```

#### 5. Web UI中的指标展示

**CheckpointStatsHandler** - 检查点统计处理器
```java
// 位置：flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/job/checkpoints/CheckpointStatsHandler.java
public class CheckpointStatsHandler extends AbstractJobHandler<CheckpointStatsRequestBody, CheckpointStatsInfo, CheckpointStatsMessageParameters> {
    
    @Override
    protected CheckpointStatsInfo handleRequest(
            HandlerRequest<CheckpointStatsRequestBody, CheckpointStatsMessageParameters> request,
            RestfulGateway gateway) throws RestHandlerException {
            
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        
        // 获取检查点统计信息
        CompletableFuture<CheckpointStatsSnapshot> statsFuture = 
            gateway.requestCheckpointStats(jobId, Time.seconds(10));
            
        CheckpointStatsSnapshot stats = statsFuture.get();
        
        // 转换为REST API响应格式
        return CheckpointStatsInfo.createFrom(stats);
    }
}
```

### 关键配置和使用方式

#### 1. 启用检查点指标收集
```java
// 在Flink配置中启用指标
Configuration config = new Configuration();
config.setString("metrics.reporters", "promethues");
config.setString("metrics.reporter.prometheus.class", "org.apache.flink.metrics.prometheus.PrometheusReporter");

// 启用检查点
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(60000); // 每60秒执行一次检查点
```

#### 2. 自定义检查点指标
```java
public class CustomCheckpointMetrics extends RichFunction {
    
    private transient Counter checkpointCounter;
    private transient Histogram checkpointSizeHistogram;
    
    @Override
    public void open(Configuration parameters) {
        this.checkpointCounter = getRuntimeContext()
            .getMetricGroup()
            .counter("custom_checkpoint_count");
            
        this.checkpointSizeHistogram = getRuntimeContext()
            .getMetricGroup()
            .histogram("custom_checkpoint_size", new DescriptiveStatisticsHistogram(1000));
    }
    
    public void reportCustomCheckpointMetric(long checkpointSize) {
        checkpointCounter.inc();
        checkpointSizeHistogram.update(checkpointSize);
    }
}
```

### 源码获取方式

要获取完整的Flink 1.14源码，可以通过以下方式：

1. **GitHub仓库克隆**：
```bash
git clone https://github.com/apache/flink.git
cd flink
git checkout release-1.14
```

2. **查看具体文件**：
   - CheckPoint相关代码主要在 `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/` 目录
   - Metrics相关代码在 `flink-runtime/src/main/java/org/apache/flink/runtime/metrics/` 目录
   - Web UI代码在 `flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/` 目录

3. **在线查看**：
   - Apache Flink官方GitHub: https://github.com/apache/flink/tree/release-1.14
   - 检查点代码目录: https://github.com/apache/flink/tree/release-1.14/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint

### 相关文档

- [Flink CheckPoint文档](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/state/checkpoints/)
- [Flink Metrics文档](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/metrics/)
- [Flink监控指南](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/monitoring/)

## 总结

Flink 1.14的CheckPoint Metrics上报代码主要包含：
1. **CheckpointStatsTracker** - 负责统计信息跟踪
2. **CheckpointMetrics** - 存储检查点指标数据
3. **CheckpointCoordinator** - 协调检查点执行并上报指标
4. **MetricGroup** - 将指标集成到Flink的指标系统
5. **Web UI Handler** - 提供REST API查询检查点统计信息

这些代码协同工作，确保检查点的执行情况能够被有效监控和报告。

---

**参考链接**：
- Apache Flink官方网站: https://flink.apache.org/
- Flink 1.14 GitHub源码: https://github.com/apache/flink/tree/release-1.14