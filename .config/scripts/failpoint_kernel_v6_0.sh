#!/usr/bin/env bash

# Exit with 1 if NEXTEST_ENV isn't defined.
if [ -z "$NEXTEST_ENV" ]; then
    exit 1
fi

# Write out an environment variable to $NEXTEST_ENV.
echo "FAILPOINTS=kernel_v6_0=return" >> "$NEXTEST_ENV"