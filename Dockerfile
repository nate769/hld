# Shared image for the App Nodes, Load Balancer and Seeder.
# The runtime `command` is chosen per-service in docker-compose.yml.
FROM oven/bun:1

WORKDIR /app

# Install first (cached layer). The project has no runtime deps, but this keeps
# types/tooling consistent and the lockfile honoured.
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# App source, frontend and the seed dataset.
COPY . .

# App nodes listen here inside the container (compose sets PORT/SHARD_ID).
EXPOSE 3000
