# Stage 1: Build the React application
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Stage 2: Serve the static files with Nginx
FROM nginx:1.25-alpine
# Copy the built assets from the 'builder' stage
COPY --from=builder /app/dist /usr/share/nginx/html
# Copy the Nginx configuration file from this same directory
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]