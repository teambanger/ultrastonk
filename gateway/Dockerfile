# Gateway image: Node.js + production dependencies only.
FROM node:24-alpine

WORKDIR /app
ENV NODE_ENV=production

# Dependencies first, so code changes don't reinstall them (layer cache).
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src
COPY public ./public

# Defaults for running inside the team's docker network; override with -e / compose.
ENV PORT=8080 \
    SOURCE=kafka \
    KAFKA_BROKERS=kafka:29092 \
    KAFKA_TOPIC=event-processed

EXPOSE 8080
USER node

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/health >/dev/null || exit 1

# exec form: node is PID 1 and receives SIGTERM from `docker stop` (clean shutdown).
CMD ["node", "src/index.js"]
