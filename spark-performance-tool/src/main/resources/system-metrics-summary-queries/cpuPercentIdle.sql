select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentIdle on queryMetrics.queryId = cpuPercentIdle.queryId where
      cpuPercentIdle.epochInMilliSeconds >= startTime and
      cpuPercentIdle.epochInMilliSeconds <= endTime