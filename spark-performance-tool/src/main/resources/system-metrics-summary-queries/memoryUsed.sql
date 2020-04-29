select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      memoryUsed on queryMetrics.queryId = memoryUsed.queryId where
      memoryUsed.epochInMilliSeconds >= startTime and
      memoryUsed.epochInMilliSeconds <= endTime