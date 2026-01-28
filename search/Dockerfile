FROM typesense/typesense:30.0

# Use bash explicitly
SHELL ["/bin/bash", "-c"]

# Install dependencies for entrypoint script
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        curl \
        jq \
    && rm -rf /var/lib/apt/lists/*

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

HEALTHCHECK --start-period=30s --start-interval=1s CMD curl --silent --fail http://localhost:$PORT/health || exit 1

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
