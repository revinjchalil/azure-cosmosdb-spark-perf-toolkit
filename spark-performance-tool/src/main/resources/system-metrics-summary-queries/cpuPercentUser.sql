select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentUser on queryMetrics.queryId = cpuPercentUser.queryId where
      cpuPercentUser.epochInMilliSeconds >= startTime and
      cpuPercentUser.epochInMilliSeconds <= endTime