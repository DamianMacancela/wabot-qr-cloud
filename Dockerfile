FROM node:22-alpine

WORKDIR /app

# Dependencias necesarias para compilar librerías nativas si se requieren
RUN apk add --no-cache python3 make g++ git

# Copiar manifiestos
COPY package*.json ./

# Instalar dependencias de producción
RUN npm install --omit=dev

# Copiar el código fuente
COPY . .

# Exponer el puerto configurado por el host de la nube
EXPOSE 3000

ENV PORT=3000
ENV NODE_ENV=production

# Comando de inicio
CMD ["node", "server.js"]
