select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value
      from queryMetrics left join diskPendingOperations on queryMetrics.queryId = diskPendingOperations.queryId
      where diskPendingOperations.epochInMilliSeconds >= startTime and
      diskPendingOperations.epochInMilliSeconds <= endTime