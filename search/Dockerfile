FROM typesense/typesense:29.0

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

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
