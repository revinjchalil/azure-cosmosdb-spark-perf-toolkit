select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentSteal on queryMetrics.queryId = cpuPercentSteal.queryId where
      cpuPercentSteal.epochInMilliSeconds >= startTime and
      cpuPercentSteal.epochInMilliSeconds <= endTime