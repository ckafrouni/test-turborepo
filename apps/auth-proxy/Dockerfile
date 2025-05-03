# Dockerfile for auth-proxy app

# 1. Base Node image
FROM node:20-slim AS base
ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable
WORKDIR /app

# 2. Install dependencies only when needed
FROM base AS dependencies
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json .npmrc* ./
RUN --mount=type=cache,target=/pnpm/store pnpm install --frozen-lockfile --prod=false

# 3. Prune the workspace
FROM base AS pruner
WORKDIR /app
COPY . .
COPY --from=dependencies /app/node_modules ./node_modules
# Generate the pruned workspace
RUN pnpm turbo prune @acme/auth-proxy --docker

# 4. Build the application
FROM base AS builder
WORKDIR /app

# Copy pruned package.json files and lockfile
COPY --from=pruner /app/out/json/ .
COPY --from=pruner /app/out/pnpm-lock.yaml ./pnpm-lock.yaml
# Copy pruned source code
COPY --from=pruner /app/out/full/ .
# Copy turbo configuration if needed by build
# COPY --from=pruner /app/out/turbo.json .

# Install dependencies for the pruned workspace
RUN --mount=type=cache,target=/pnpm/store pnpm install --frozen-lockfile --prod=false

# Build the specific app
RUN pnpm build --filter=@acme/auth-proxy...

# 5. Final image
FROM base AS runner
WORKDIR /app

# Set environment variables (adjust if needed)
ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

# Copy necessary files from the builder stage
COPY --from=builder /app/apps/auth-proxy/.output ./apps/auth-proxy/.output
# Copy node_modules necessary for running the app (often needed for runtime dependencies)
COPY --from=builder /app/out/json/ ./json/
COPY --from=builder /app/out/pnpm-lock.yaml ./pnpm-lock.yaml
COPY --from=builder /app/node_modules ./node_modules
# Reconstruct the pruned node_modules
RUN --mount=type=cache,target=/pnpm/store pnpm install --frozen-lockfile --prod --offline

# Copy the built app files
COPY --from=builder /app/apps/auth-proxy/package.json ./apps/auth-proxy/package.json
# If other pruned packages are needed at runtime, copy them too.
# Example: COPY --from=builder /app/packages/auth ./packages/auth

EXPOSE 3000

CMD [ "node", "apps/auth-proxy/.output/server/index.mjs" ]
