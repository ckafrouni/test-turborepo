# Dockerfile for nextjs app

# 1. Base Node image
FROM node:23-slim AS base
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
# Generate the pruned workspace tailored for the nextjs app
RUN pnpm turbo prune @acme/nextjs --docker

# 4. Build the application
FROM base AS builder
WORKDIR /app

# Copy pruned package.json files and lockfile
COPY --from=pruner /app/out/json/ .
COPY --from=pruner /app/out/pnpm-lock.yaml ./pnpm-lock.yaml
# Copy pruned source code
COPY --from=pruner /app/out/full/ .
# Copy turbo configuration if needed by build
# COPY --from=pruner /app/out/turbo.json ./turbo.json

# Install dependencies for the pruned workspace
RUN --mount=type=cache,target=/pnpm/store pnpm install --frozen-lockfile --prod=false

# Required for skipping linting and typechecking
ENV CI=true

# Build the specific app
RUN pnpm build --filter=@acme/nextjs...

# 5. Final image for Next.js runtime
FROM base AS runner
WORKDIR /app

ENV NODE_ENV=production
# Uncomment the following line in case you want to disable telemetry during runtime.
# ENV NEXT_TELEMETRY_DISABLED 1

RUN groupadd --system --gid 1001 nodejs
RUN useradd --system --uid 1001 nextjs

COPY --from=builder /app/apps/nextjs/public ./apps/nextjs/public

# Automatically leverage output traces to reduce image size
# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=builder --chown=nextjs:nodejs /app/apps/nextjs/.next/standalone ./ 
COPY --from=builder --chown=nextjs:nodejs /app/apps/nextjs/.next/static ./apps/nextjs/.next/static

USER nextjs

EXPOSE 3000

ENV PORT=3000
# set hostname to localhost
ENV HOSTNAME="0.0.0.0"

CMD ["node", "apps/nextjs/server.js"]
