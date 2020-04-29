select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentNice on queryMetrics.queryId = cpuPercentNice.queryId where
      cpuPercentNice.epochInMilliSeconds >= startTime and
      cpuPercentNice.epochInMilliSeconds <= endTime