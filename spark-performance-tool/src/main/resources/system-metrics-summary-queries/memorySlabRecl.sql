select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      memorySlabRecl on queryMetrics.queryId = memorySlabRecl.queryId where
      memorySlabRecl.epochInMilliSeconds >= startTime and
      memorySlabRecl.epochInMilliSeconds <= endTime