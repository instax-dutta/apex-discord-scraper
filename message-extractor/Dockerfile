FROM node:20

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci --only=production && npm cache clean --force

# Copy compiled code
COPY dist/ ./dist/

# Create data directory
RUN mkdir -p /app/data

# Run with exposure to GC
ENV NODE_OPTIONS="--expose-gc"

# Override with e.g. `docker run <image> extract <channel_id>`
CMD ["node", "dist/cli.js", "status"]
