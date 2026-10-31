# Stage 1: Build the React + TypeScript SPA with Vite
FROM node:20-alpine AS build
WORKDIR /app

COPY dashboard/package.json ./
RUN npm install --no-audit --no-fund

COPY dashboard/index.html dashboard/tsconfig.json dashboard/vite.config.ts ./
COPY dashboard/src ./src
RUN npm run build

# Stage 2: Serve static assets with Nginx and reverse-proxy /api/* to controller:8080
FROM nginx:1.27-alpine
COPY dashboard/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD wget -q -O /dev/null http://127.0.0.1:80/ || exit 1

