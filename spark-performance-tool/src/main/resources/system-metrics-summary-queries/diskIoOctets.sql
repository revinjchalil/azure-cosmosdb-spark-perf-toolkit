select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, read, write
      from queryMetrics left join diskIoOctets on queryMetrics.queryId = diskIoOctets.queryId
      where diskIoOctets.epochInMilliSeconds >= startTime and
      diskIoOctets.epochInMilliSeconds <= endTime