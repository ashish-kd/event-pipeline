#!/bin/bash
# Generate self-signed certificates for development/testing
# In production, use proper CA-signed certificates

set -e

CERT_DIR="./certs"
DOMAIN="localhost"

echo "🔒 Generating TLS certificates for development..."

# Create certificate directory
mkdir -p $CERT_DIR

# Generate private key
openssl genrsa -out $CERT_DIR/server.key 2048

# Generate certificate signing request
openssl req -new -key $CERT_DIR/server.key -out $CERT_DIR/server.csr -subj "/C=US/ST=State/L=City/O=Organization/CN=$DOMAIN"

# Generate self-signed certificate
openssl x509 -req -days 365 -in $CERT_DIR/server.csr -signkey $CERT_DIR/server.key -out $CERT_DIR/server.crt

# Generate certificate for Kafka (if needed)
openssl genrsa -out $CERT_DIR/kafka.key 2048
openssl req -new -key $CERT_DIR/kafka.key -out $CERT_DIR/kafka.csr -subj "/C=US/ST=State/L=City/O=Organization/CN=kafka"
openssl x509 -req -days 365 -in $CERT_DIR/kafka.csr -signkey $CERT_DIR/kafka.key -out $CERT_DIR/kafka.crt

# Generate certificate for PostgreSQL
openssl genrsa -out $CERT_DIR/postgres.key 2048
openssl req -new -key $CERT_DIR/postgres.key -out $CERT_DIR/postgres.csr -subj "/C=US/ST=State/L=City/O=Organization/CN=postgres"
openssl x509 -req -days 365 -in $CERT_DIR/postgres.csr -signkey $CERT_DIR/postgres.key -out $CERT_DIR/postgres.crt

# Set proper permissions
chmod 600 $CERT_DIR/*.key
chmod 644 $CERT_DIR/*.crt

# Create combined certificate file for some applications
cat $CERT_DIR/server.crt $CERT_DIR/server.key > $CERT_DIR/server.pem

echo "✅ Certificates generated successfully in $CERT_DIR/"
echo "📋 Files created:"
echo "   • server.crt, server.key - Web services"
echo "   • kafka.crt, kafka.key - Kafka SSL"
echo "   • postgres.crt, postgres.key - PostgreSQL SSL"
echo "   • server.pem - Combined certificate file"
echo ""
echo "⚠️  Note: These are self-signed certificates for development only."
echo "   In production, use certificates from a trusted CA."
