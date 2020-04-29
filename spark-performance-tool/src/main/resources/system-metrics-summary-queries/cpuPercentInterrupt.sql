select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentInterrupt on queryMetrics.queryId = cpuPercentInterrupt.queryId where
      cpuPercentInterrupt.epochInMilliSeconds >= startTime and
      cpuPercentInterrupt.epochInMilliSeconds <= endTime