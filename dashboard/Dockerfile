# Build context for this Dockerfile is the `dashboard/` directory itself
# (see docker-compose.yml: build.context = ./dashboard), NOT the repo root.
# That keeps this build's context small and lets dashboard/.dockerignore
# actually apply — a generic root-level .dockerignore only protects
# services whose context is the repo root.
FROM node:20-slim AS build
WORKDIR /app

# Install dependencies first (better layer caching) using the lockfile for
# a reproducible, from-scratch install — this always installs fresh inside
# the container; it never uses anything from the host's node_modules,
# which isn't even sent to the daemon thanks to .dockerignore.
COPY package.json package-lock.json ./
RUN npm ci

COPY . ./
ARG VITE_API_BASE_URL=http://localhost:8000
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
