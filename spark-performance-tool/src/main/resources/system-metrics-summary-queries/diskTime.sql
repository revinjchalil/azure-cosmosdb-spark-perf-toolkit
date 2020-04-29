select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, read, write
      from queryMetrics left join diskTime on queryMetrics.queryId = diskTime.queryId
      where diskTime.epochInMilliSeconds >= startTime and
      diskTime.epochInMilliSeconds <= endTime