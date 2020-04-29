select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, tx, rx  from queryMetrics
      LEFT JOIN networkOctets ON queryMetrics.queryId = networkOctets.queryId WHERE
      networkOctets.epochInMilliSeconds >= startTime AND
      networkOctets.epochInMilliSeconds <= endTime