# syntax=docker/dockerfile:1

# Build stage. Compiles TypeScript so the image works from a fresh clone, where
# dist/ does not exist yet (it is gitignored).
FROM node:20-slim AS build

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# Drop dev dependencies from the tree we are about to copy.
RUN npm prune --omit=dev

# Runtime stage. Only the compiled output and the production dependency tree
# are carried forward, so no sources, tests, or build tooling ship in the image.
FROM node:20-slim AS runtime

WORKDIR /app

ENV NODE_ENV=production
# Exposed so the archive can release memory back to the OS under long backfills.
ENV NODE_OPTIONS="--expose-gc"

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json ./
COPY --from=build /app/schema.sql ./
COPY .env.example ./

# The archive lives on a volume, never in the image layer.
RUN mkdir -p /app/data && chown -R node:node /app/data
VOLUME ["/app/data"]
USER node

ENTRYPOINT ["node", "dist/cli.js"]
CMD ["status"]
