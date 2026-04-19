FROM node:22-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine AS runtime

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000

COPY package*.json ./
RUN apk add --no-cache unzip
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist
COPY sample.json ./sample.json
COPY sample.zip ./sample.zip

EXPOSE 3000

CMD ["node", "dist/index.js"]