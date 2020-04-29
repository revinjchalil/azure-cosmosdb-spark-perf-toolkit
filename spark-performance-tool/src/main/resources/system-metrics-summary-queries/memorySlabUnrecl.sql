select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      memorySlabUnrecl on queryMetrics.queryId = memorySlabUnrecl.queryId where
      memorySlabUnrecl.epochInMilliSeconds >= startTime and
      memorySlabUnrecl.epochInMilliSeconds <= endTime