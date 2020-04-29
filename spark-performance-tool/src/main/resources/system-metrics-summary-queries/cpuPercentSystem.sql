select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentSystem on queryMetrics.queryId = cpuPercentSystem.queryId where
      cpuPercentSystem.epochInMilliSeconds >= startTime and
      cpuPercentSystem.epochInMilliSeconds <= endTime