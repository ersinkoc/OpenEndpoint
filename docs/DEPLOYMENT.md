# Deployment Guide

Complete deployment documentation for OpenEndpoint.

## Table of Contents

- [System Requirements](#system-requirements)
- [Docker Deployment](#docker-deployment)
- [Binary Deployment](#binary-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Systemd Service](#systemd-service)
- [Reverse Proxy Setup](#reverse-proxy-setup)
- [SSL/TLS Configuration](#ssltls-configuration)
- [Production Checklist](#production-checklist)
- [Monitoring & Alerting](#monitoring--alerting)
- [Backup & Recovery](#backup--recovery)

---

## System Requirements

### Minimum Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 4 GB | 8+ GB |
| Disk | 50 GB SSD | 200+ GB SSD |
| Network | 100 Mbps | 1 Gbps |

### Operating Systems

- Linux (Ubuntu 20.04+, CentOS 8+, Debian 11+)
- macOS 12+
- Windows Server 2019+

### Dependencies

- Go 1.22+ (for building from source)
- Docker 20.10+ (for containerized deployment)
- curl/wget (for health checks)

---

## Docker Deployment

### Quick Start

```bash
# Run with Docker
docker run -d \
  --name openendpoint \
  -p 9000:9000 \
  -e OPENEP_AUTH_ACCESS_KEY=minioadmin \
  -e OPENEP_AUTH_SECRET_KEY=minioadmin \
  -v /data/openendpoint:/data \
  --restart unless-stopped \
  openendpoint/openendpoint:1.0.0
```

### Docker Compose

```yaml
version: '3.8'

services:
  openendpoint:
    image: openendpoint/openendpoint:1.0.0
    container_name: openendpoint
    ports:
      - "9000:9000"
    environment:
      - OPENEP_AUTH_ACCESS_KEY=minioadmin
      - OPENEP_AUTH_SECRET_KEY=minioadmin
      - OPENEP_SERVER_HOST=0.0.0.0
      - OPENEP_SERVER_PORT=9000
      - OPENEP_STORAGE_DATA_DIR=/data
      - OPENEP_LOGGING_LEVEL=info
    volumes:
      - ./data:/data
      - ./config.yaml:/app/config.yaml:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    networks:
      - openendpoint-net

  # Optional: Prometheus for metrics
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    networks:
      - openendpoint-net

networks:
  openendpoint-net:
    driver: bridge
```

### Docker Swarm

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.yml openendpoint

# Check status
docker stack ps openendpoint
docker service logs openendpoint_openendpoint
```

---

## Binary Deployment

### Download and Install

```bash
# Create user
sudo useradd -r -s /bin/false openendpoint

# Create directories
sudo mkdir -p /opt/openendpoint/bin
sudo mkdir -p /opt/openendpoint/data
sudo mkdir -p /opt/openendpoint/config
sudo mkdir -p /var/log/openendpoint

# Download binary
curl -sL https://github.com/openendpoint/openendpoint/releases/download/v1.0.0/openep-linux-amd64.tar.gz | \
  sudo tar xz -C /opt/openendpoint/bin

# Set permissions
sudo chown -R openendpoint:openendpoint /opt/openendpoint
sudo chown -R openendpoint:openendpoint /var/log/openendpoint
sudo chmod +x /opt/openendpoint/bin/openep
```

### Configuration

```bash
# Create config
sudo tee /opt/openendpoint/config/config.yaml > /dev/null << 'EOF'
server:
  host: "0.0.0.0"
  port: 9000
  read_timeout: 30
  write_timeout: 30
  idle_timeout: 120

auth:
  access_key: "your-access-key"
  secret_key: "your-secret-key"

storage:
  data_dir: "/opt/openendpoint/data"

logging:
  level: "info"
  format: "json"
  output: "/var/log/openendpoint/app.log"

audit:
  enabled: true
  path: "/var/log/openendpoint/audit"
EOF

sudo chown openendpoint:openendpoint /opt/openendpoint/config/config.yaml
```

### Start Service

```bash
# Run directly
sudo -u openendpoint /opt/openendpoint/bin/openep server \
  --config /opt/openendpoint/config/config.yaml

# Or use systemd (see below)
```

---

## Kubernetes Deployment

### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: openendpoint
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: openendpoint-config
  namespace: openendpoint
data:
  config.yaml: |
    server:
      host: "0.0.0.0"
      port: 9000
    auth:
      access_key: "$(ACCESS_KEY)"
      secret_key: "$(SECRET_KEY)"
    storage:
      data_dir: "/data"
    logging:
      level: "info"
      format: "json"
```

### Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: openendpoint-secrets
  namespace: openendpoint
type: Opaque
stringData:
  access-key: "your-access-key"
  secret-key: "your-secret-key"
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: openendpoint
  namespace: openendpoint
spec:
  replicas: 1
  selector:
    matchLabels:
      app: openendpoint
  template:
    metadata:
      labels:
        app: openendpoint
    spec:
      containers:
        - name: openendpoint
          image: openendpoint/openendpoint:1.0.0
          ports:
            - containerPort: 9000
              name: http
          env:
            - name: OPENEP_AUTH_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: openendpoint-secrets
                  key: access-key
            - name: OPENEP_AUTH_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: openendpoint-secrets
                  key: secret-key
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              memory: "4Gi"
              cpu: "2"
            limits:
              memory: "8Gi"
              cpu: "4"
          livenessProbe:
            httpGet:
              path: /health
              port: 9000
            initialDelaySeconds: 30
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /ready
              port: 9000
            initialDelaySeconds: 10
            periodSeconds: 10
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: openendpoint-data
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: openendpoint
  namespace: openendpoint
spec:
  selector:
    app: openendpoint
  ports:
    - port: 9000
      targetPort: 9000
  type: ClusterIP
```

### Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: openendpoint
  namespace: openendpoint
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "5g"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "300"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "300"
spec:
  tls:
    - hosts:
        - s3.yourdomain.com
      secretName: openendpoint-tls
  rules:
    - host: s3.yourdomain.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: openendpoint
                port:
                  number: 9000
```

### PersistentVolumeClaim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: openendpoint-data
  namespace: openendpoint
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
  storageClassName: standard
```

### Apply

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f pvc.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml
```

---

## Systemd Service

### Service File

```bash
sudo tee /etc/systemd/system/openendpoint.service > /dev/null << 'EOF'
[Unit]
Description=OpenEndpoint Object Storage
Documentation=https://openendpoint.com/docs
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=openendpoint
Group=openendpoint
ExecStart=/opt/openendpoint/bin/openep server --config /opt/openendpoint/config/config.yaml
Restart=on-failure
RestartSec=5
StartLimitInterval=60s
StartLimitBurst=3

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/openendpoint/data /var/log/openendpoint

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=openendpoint

[Install]
WantedBy=multi-user.target
EOF
```

### Manage Service

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable service
sudo systemctl enable openendpoint

# Start service
sudo systemctl start openendpoint

# Check status
sudo systemctl status openendpoint

# View logs
sudo journalctl -u openendpoint -f
```

---

## Reverse Proxy Setup

### Nginx

```nginx
server {
    listen 80;
    server_name s3.yourdomain.com;

    # Redirect to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name s3.yourdomain.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    # Large file uploads
    client_max_body_size 5G;
    proxy_request_buffering off;

    # Timeouts
    proxy_connect_timeout 300s;
    proxy_send_timeout 300s;
    proxy_read_timeout 300s;

    location / {
        proxy_pass http://localhost:9000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Traefik

```yaml
# docker-compose.yml
version: '3.8'

services:
  traefik:
    image: traefik:v2.10
    command:
      - "--api.insecure=true"
      - "--providers.docker=true"
      - "--entrypoints.web.address=:80"
      - "--entrypoints.websecure.address=:443"
      - "--certificatesresolvers.letsencrypt.acme.tlschallenge=true"
      - "--certificatesresolvers.letsencrypt.acme.email=admin@yourdomain.com"
      - "--certificatesresolvers.letsencrypt.acme.storage=/letsencrypt/acme.json"
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./letsencrypt:/letsencrypt

  openendpoint:
    image: openendpoint/openendpoint:1.0.0
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.openendpoint.rule=Host(`s3.yourdomain.com`)"
      - "traefik.http.routers.openendpoint.tls=true"
      - "traefik.http.routers.openendpoint.tls.certresolver=letsencrypt"
      - "traefik.http.services.openendpoint.loadbalancer.server.port=9000"
```

---

## SSL/TLS Configuration

### Let's Encrypt

```bash
# Install certbot
sudo apt install certbot

# Obtain certificate
sudo certbot certonly --standalone -d s3.yourdomain.com

# Auto-renewal
sudo certbot renew --dry-run
```

### Self-Signed Certificate

```bash
# Generate private key
openssl genrsa -out server.key 2048

# Generate certificate
openssl req -new -x509 -sha256 -key server.key -out server.crt -days 365
```

---

## Production Checklist

### Security

- [ ] Change default access/secret keys
- [ ] Enable audit logging
- [ ] Configure firewall rules
- [ ] Set up SSL/TLS certificates
- [ ] Enable rate limiting
- [ ] Configure CORS policies
- [ ] Set up intrusion detection

### Performance

- [ ] Allocate sufficient RAM (8GB+)
- [ ] Use SSD storage
- [ ] Configure appropriate timeouts
- [ ] Enable compression if needed
- [ ] Set up CDN for static assets

### Monitoring

- [ ] Configure Prometheus metrics
- [ ] Set up Grafana dashboards
- [ ] Configure log aggregation
- [ ] Set up alerting rules
- [ ] Monitor disk usage

### Backup

- [ ] Configure automated backups
- [ ] Test restore procedures
- [ ] Set up off-site replication
- [ ] Document recovery procedures

### High Availability

- [ ] Set up load balancer
- [ ] Configure multiple instances
- [ ] Set up database replication
- [ ] Test failover procedures

---

## Monitoring & Alerting

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'openendpoint'
    static_configs:
      - targets: ['openendpoint:9000']
    metrics_path: /metrics
```

### Grafana Dashboard

Import dashboard ID `12345` or create custom dashboard with:
- Request rate
- Error rate
- Response time
- Storage usage
- Active connections

### Alerting Rules

```yaml
# alerts.yml
groups:
  - name: openendpoint
    rules:
      - alert: HighErrorRate
        expr: rate(openendpoint_requests_failed_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"

      - alert: DiskSpaceLow
        expr: openendpoint_storage_disk_usage_percent > 80
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Disk space is running low"
```

---

## Backup & Recovery

### Automated Backup

```bash
#!/bin/bash
# backup.sh

BACKUP_DIR="/backup/openendpoint"
DATE=$(date +%Y%m%d_%H%M%S)
DATA_DIR="/opt/openendpoint/data"

# Create backup
mkdir -p $BACKUP_DIR
tar czf $BACKUP_DIR/backup_$DATE.tar.gz $DATA_DIR

# Keep only last 7 backups
ls -t $BACKUP_DIR/backup_*.tar.gz | tail -n +8 | xargs rm -f
```

### Recovery

```bash
#!/bin/bash
# restore.sh

BACKUP_FILE=$1
DATA_DIR="/opt/openendpoint/data"

# Stop service
sudo systemctl stop openendpoint

# Restore data
sudo rm -rf $DATA_DIR/*
sudo tar xzf $BACKUP_FILE -C /

# Fix permissions
sudo chown -R openendpoint:openendpoint $DATA_DIR

# Start service
sudo systemctl start openendpoint
```

### Cron Job

```bash
# Edit crontab
sudo crontab -e

# Add backup job (daily at 2 AM)
0 2 * * * /opt/openendpoint/scripts/backup.sh
```

---

## Troubleshooting

### Common Issues

#### Port Already in Use

```bash
# Find process
sudo lsof -i :9000

# Kill process
sudo kill -9 <PID>
```

#### Permission Denied

```bash
# Fix permissions
sudo chown -R openendpoint:openendpoint /opt/openendpoint
```

#### Out of Memory

```bash
# Check memory usage
free -h

# Check OOM kills
dmesg | grep -i "out of memory"
```

#### Disk Full

```bash
# Check disk usage
df -h

# Find large files
sudo du -h /opt/openendpoint/data | sort -rh | head -20
```

---

## Additional Resources

- [OpenEndpoint README](../README.md)
- [Testing Guide](TESTING.md)
- [Error Codes Reference](ERROR_CODES.md)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
