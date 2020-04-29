select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, io_time, weighted_io_time
      from queryMetrics left join diskIoTime on queryMetrics.queryId = diskIoTime.queryId
      where diskIoTime.epochInMilliSeconds >= startTime and
      diskIoTime.epochInMilliSeconds <= endTime