select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, read, write
      from queryMetrics left join diskIoMerged on queryMetrics.queryId = diskIoMerged.queryId
      where diskIoMerged.epochInMilliSeconds >= startTime and
      diskIoMerged.epochInMilliSeconds <= endTime