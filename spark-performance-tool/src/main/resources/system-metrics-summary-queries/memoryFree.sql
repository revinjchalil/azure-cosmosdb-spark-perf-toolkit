select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      memoryFree on queryMetrics.queryId = memoryFree.queryId where
      memoryFree.epochInMilliSeconds >= startTime and
      memoryFree.epochInMilliSeconds <= endTime