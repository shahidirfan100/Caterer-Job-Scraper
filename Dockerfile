# Use Apify's Node.js image with Playwright pre-installed
FROM apify/actor-node-playwright-chrome:20

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy source code
COPY . ./

# Run the actor
CMD npm start
