select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, tx, rx  from queryMetrics
      left join networkErrors on queryMetrics.queryId = networkErrors.queryId where
      networkErrors.epochInMilliSeconds >= startTime and
      networkErrors.epochInMilliSeconds <= endTime