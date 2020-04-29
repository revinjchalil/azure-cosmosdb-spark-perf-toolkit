select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentWait on queryMetrics.queryId = cpuPercentWait.queryId where
      cpuPercentWait.epochInMilliSeconds >= startTime and
      cpuPercentWait.epochInMilliSeconds <= endTime