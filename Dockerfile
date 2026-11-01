FROM node:24-alpine AS build
WORKDIR /app
COPY package*.json ./

RUN npm ci
COPY tsconfig.json ./
COPY src ./src
COPY migrations ./migrations
RUN npm run build
FROM build AS production-deps
RUN npm prune --omit=dev --ignore-scripts
FROM node:24-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY --from=production-deps --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --from=build --chown=node:node /app/package*.json ./
COPY --from=build --chown=node:node /app/migrations ./migrations
USER node
EXPOSE 3000
CMD ["node","dist/main.js"]
