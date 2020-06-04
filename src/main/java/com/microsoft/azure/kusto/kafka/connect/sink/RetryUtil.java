package com.microsoft.azure.kusto.kafka.connect.sink;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RetryUtil {

  /**
   * An arbitrary absolute maximum practical retry time.
   */
  public static final long MAX_RETRY_TIME_MS = TimeUnit.HOURS.toMillis(24);

  /**
   * Compute the time to sleep using exponential backoff with jitter. This method computes the
   * normal exponential backoff as {@code initialRetryBackoffMs << retryAttempt}, and then
   * chooses a random value between 0 and that value.
   *
   * @param retryAttempts         the number of previous retry attempts; must be non-negative
   * @param initialRetryBackoffMs the initial time to wait before retrying; assumed to
   *                              be 0 if value is negative
   * @return the non-negative time in milliseconds to wait before the next retry attempt,
   *         or 0 if {@code initialRetryBackoffMs} is negative
   */
  public static long computeRandomRetryWaitTimeInMillis(int retryAttempts,
                                                        long initialRetryBackoffMs) {
    if (initialRetryBackoffMs < 0) {
      return 0;
    }
    if (retryAttempts < 0) {
      return initialRetryBackoffMs;
    }
    long maxRetryTime = computeRetryWaitTimeInMillis(retryAttempts, initialRetryBackoffMs);
    return ThreadLocalRandom.current().nextLong(0, maxRetryTime);
  }

  /**
   * Compute the time to sleep using exponential backoff. This method computes the normal
   * exponential backoff as {@code initialRetryBackoffMs << retryAttempt}, bounded to always
   * be less than {@link #MAX_RETRY_TIME_MS}.
   *
   * @param retryAttempts         the number of previous retry attempts; must be non-negative
   * @param initialRetryBackoffMs the initial time to wait before retrying; assumed to be 0
   *                              if value is negative
   * @return the non-negative time in milliseconds to wait before the next retry attempt,
   *         or 0 if {@code initialRetryBackoffMs} is negative
   */
  public static long computeRetryWaitTimeInMillis(int retryAttempts,
                                                  long initialRetryBackoffMs) {
    if (initialRetryBackoffMs < 0) {
      return 0;
    }
    if (retryAttempts <= 0) {
      return initialRetryBackoffMs;
    }
    if (retryAttempts > 32) {
      // This would overflow the exponential algorithm ...
      return MAX_RETRY_TIME_MS;
    }
    long result = initialRetryBackoffMs << retryAttempts;
    return result < 0L ? MAX_RETRY_TIME_MS : Math.min(MAX_RETRY_TIME_MS, result);
  }


}
