FROM apify/actor-node-playwright-chrome:22-1.54.1

WORKDIR /home/myuser/app

# Install dependencies first to leverage Docker layer cache
COPY package*.json ./
RUN npm ci --omit=dev

# Copy actor source
COPY . ./

# Default command: run the actor
CMD ["npm", "start"]
