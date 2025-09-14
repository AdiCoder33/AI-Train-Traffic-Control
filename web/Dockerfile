FROM node:20-alpine as build
WORKDIR /web
COPY package.json package-lock.json tsconfig.json tsconfig.node.json vite.config.ts /web/
COPY src /web/src
COPY index.html /web/
COPY styles.css /web/
RUN npm ci && npm run build

FROM nginx:alpine
COPY --from=build /web/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

