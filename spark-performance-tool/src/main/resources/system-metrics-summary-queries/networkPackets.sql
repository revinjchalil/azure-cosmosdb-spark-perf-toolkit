select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, tx, rx  from queryMetrics
      left join networkPackets on queryMetrics.queryId = networkPackets.queryId where
      networkPackets.epochInMilliSeconds >= startTime and
      networkPackets.epochInMilliSeconds <= endTime