Attempt to restart an interrupted download instead of raising a 'changeset was
truncated in transit' error. The newly added downloadAttempts,
downloadRetryThreshold, and downloadRetryTrim configuration options govern this
behavior.
