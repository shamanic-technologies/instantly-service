# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts
COPY drizzle ./drizzle
RUN npm run build

# Production stage
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/openapi.json ./openapi.json
COPY --from=builder /app/drizzle ./drizzle

ENV NODE_ENV=production
ENV PORT=3011

EXPOSE 3011

CMD ["node", "dist/index.js"]
