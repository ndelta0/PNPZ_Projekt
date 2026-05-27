FROM node:22-alpine AS builder

RUN npm install -g pnpm

WORKDIR /app

ARG PUBLIC_BACKEND_URL=http://localhost:50052
ENV PUBLIC_BACKEND_URL=$PUBLIC_BACKEND_URL

COPY package.json pnpm-lock.yaml ./
RUN CI=true pnpm install --frozen-lockfile

COPY . .
RUN CI=true pnpm build

RUN CI=true pnpm prune --prod

FROM node:22-alpine

WORKDIR /app

ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

COPY --from=builder /app/build ./build
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/node_modules ./node_modules

EXPOSE 3000

CMD ["node", "build"]