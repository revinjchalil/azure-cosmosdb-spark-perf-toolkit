select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      memoryBuffered on queryMetrics.queryId = memoryBuffered.queryId where
      memoryBuffered.epochInMilliSeconds >= startTime and
      memoryBuffered.epochInMilliSeconds <= endTime