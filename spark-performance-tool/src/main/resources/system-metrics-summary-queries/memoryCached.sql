select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      memoryCached on queryMetrics.queryId = memoryCached.queryId where
      memoryCached.epochInMilliSeconds >= startTime and
      memoryCached.epochInMilliSeconds <= endTime