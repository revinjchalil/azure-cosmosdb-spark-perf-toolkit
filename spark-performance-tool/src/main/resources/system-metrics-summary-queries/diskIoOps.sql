select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, read, write  from queryMetrics
      left join diskIoOps on queryMetrics.queryId = diskIoOps.queryId where
      diskIoOps.epochInMilliSeconds >= startTime and
      diskIoOps.epochInMilliSeconds <= endTime