# DocIntel AI — Backend

A production-grade RAG (Retrieval-Augmented Generation) platform built with Spring Boot microservices, Kafka, ChromaDB, and Groq LLM. Enables private, cited document Q&A without exposing data to the cloud.

**Repository:** https://github.com/Anandbd09/doc-intelligence-platform  
**Frontend:** https://github.com/Anandbd09/docintel-ui  
**Live Demo:** https://docintel-app.netlify.app

---

## Quick Start (5 minutes)

```bash
# 1. Clone
git clone https://github.com/Anandbd09/doc-intelligence-platform.git
cd doc-intelligence-platform

# 2. Create .env file (see Configuration section)
cp .env.example .env
# Edit .env with your Groq API key

# 3. Start infrastructure
docker compose -f docker-compose.yml up -d

# 4. Start Ollama (embeddings)
ollama serve &
ollama pull nomic-embed-text &

# 5. Build and run services
mvn clean package -DskipTests

# 6. Run all 5 services (in separate terminals or background)
cd api-gateway && java -jar target/*.jar &
cd ../document-service && java -jar target/*.jar &
cd ../embedding-service && java -jar target/*.jar &
cd ../query-service && java -jar target/*.jar &
cd ../audit-service && java -jar target/*.jar &

# 7. Verify
curl http://localhost:8080/actuator/health
# Should return: {"status":"UP"}
```

Backend is now running on **http://localhost:8080**

---

## Architecture

### Microservices (4 Spring Boot applications)

| Service | Port | Responsibility | Technology |
|---------|------|-----------------|-------------|
| **API Gateway** | 8080 | Single entry point, JWT validation, CORS, routing | Spring Cloud Gateway |
| **Document Service** | 8081 | Upload, auth (register/login), S3 storage, Kafka producer | Spring Boot 3, MongoDB, AWS S3 |
| **Embedding Service** | 8082 | PDF extraction, text chunking, parallel embedding, ChromaDB storage | Spring Boot 3, Ollama, ChromaDB |
| **Query Service** | 8083 | RAG queries, LLM inference, Redis caching, summarization, comparison | Spring Boot 3, ChromaDB, Groq, Redis |
| **Audit Service** | 8084 | Kafka consumer for compliance logging | Spring Boot 3, MongoDB |

### Infrastructure (Docker Compose)

| Component | Port | Purpose | Docker Image |
|-----------|------|---------|--------------|
| **Kafka** | 9092 | Message broker for async pipeline | confluentinc/cp-kafka |
| **Zookeeper** | 2181 | Kafka coordinator | confluentinc/cp-zookeeper |
| **MongoDB** | 27017 | Document metadata, users, audit logs | mongo:7 |
| **Redis** | 6379 | Query cache, conversation memory | redis:7 |
| **ChromaDB** | 8000 | Vector database for embeddings | chromadb:latest |

### External Services

| Service | Purpose | Config |
|---------|---------|--------|
| **Ollama** | Local embeddings (nomic-embed-text) | http://localhost:11434 |
| **Groq** | LLM inference (LLaMA 3.1) | Environment variable |
| **AWS S3** | Document storage (optional) | Environment variables |
| **AWS Textract** | OCR for scanned PDFs (optional) | Environment variables |

---

## Prerequisites

### System Requirements
- **RAM:** 8GB minimum (16GB recommended for smooth embedding)
- **Disk:** 20GB free
- **CPU:** 4 cores minimum
- **OS:** Linux, macOS, Windows (WSL2)

### Software Requirements
- **Java 17+**
  ```bash
  java --version
  # Should output: openjdk version "17.x.x"
  ```
- **Maven 3.8+**
  ```bash
  mvn --version
  # Should output: Apache Maven 3.8.x
  ```
- **Docker & Docker Compose**
  ```bash
  docker --version
  docker compose version
  ```
- **Ollama**
  ```bash
  # Install from https://ollama.ai
  ollama --version
  ```

### API Keys & Credentials

**Required:**
- **Groq API Key** — [Get free here](https://console.groq.com/keys)

**Optional (for S3 + Textract):**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`
- `AWS_S3_BUCKET_NAME`

---

## Configuration

### Step 1: Create `.env` file

In the backend root directory, create `.env`:

```bash
touch .env
```

### Step 2: Add Configuration

```env
# ──────────────────────────────────────────
# REQUIRED
# ──────────────────────────────────────────

# Groq API Key (get from https://console.groq.com/keys)
GROQ_API_KEY=gsk_your_actual_key_here

# JWT Secret (use any random string, min 32 chars)
JWT_SECRET=your-secret-key-must-be-at-least-32-characters-long-123456

# ──────────────────────────────────────────
# OPTIONAL (for S3 + Textract OCR)
# ──────────────────────────────────────────

# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=ap-south-1
AWS_S3_BUCKET=your-bucket-name

# ──────────────────────────────────────────
# INTERNAL (defaults for local development)
# ──────────────────────────────────────────

# Service URLs (Docker container names)
DOCUMENT_SERVICE_URL=http://localhost:8081
QUERY_SERVICE_URL=http://localhost:8083
AUDIT_SERVICE_URL=http://localhost:8084

# Ollama
OLLAMA_BASE_URL=http://localhost:11434

# ChromaDB
CHROMADB_URL=http://localhost:8000

# MongoDB
SPRING_DATA_MONGODB_HOST=localhost
SPRING_DATA_MONGODB_PORT=27017
SPRING_DATA_MONGODB_DATABASE=docintel

# Redis
SPRING_REDIS_HOST=localhost
SPRING_REDIS_PORT=6379

# Kafka
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

## Local Development Setup

### Step 1: Start Infrastructure (Kafka, MongoDB, Redis, ChromaDB)

```bash
# From backend root
docker compose -f docker-compose.yml up -d

# Verify containers started
docker ps
```

You should see 5 containers:
- `zookeeper`
- `kafka`
- `mongodb`
- `redis`
- `chromadb`

Check they're healthy:
```bash
docker compose -f docker-compose.yml ps
```

### Step 2: Start Ollama (Local Embeddings)

Ollama runs on the **host machine**, not in Docker (for better performance).

**Linux:**
```bash
# Install
curl -fsSL https://ollama.ai/install.sh | sh

# Run
ollama serve
# In another terminal:
ollama pull nomic-embed-text
```

**macOS:**
```bash
# Download and install from https://ollama.ai/download/mac
# Then:
ollama serve
# In another terminal:
ollama pull nomic-embed-text
```

**Windows (WSL2):**
```bash
# In WSL2 terminal
curl -fsSL https://ollama.ai/install.sh | sh
wsl ollama serve
# In another WSL2 terminal:
wsl ollama pull nomic-embed-text
```

**Verify Ollama is running:**
```bash
curl http://localhost:11434/api/tags
# Should return: {"models":[{"name":"nomic-embed-text:latest",...}]}
```

### Step 3: Build All Services

```bash
# From backend root
mvn clean package -DskipTests

# This builds 5 JARs:
# - api-gateway/target/api-gateway-0.0.1-SNAPSHOT.jar
# - document-service/target/document-service-0.0.1-SNAPSHOT.jar
# - embedding-service/target/embedding-service-0.0.1-SNAPSHOT.jar
# - query-service/target/query-service-0.0.1-SNAPSHOT.jar
# - audit-service/target/audit-service-0.0.1-SNAPSHOT.jar
```

### Step 4: Run Services

**Option A — Run in separate terminals (recommended for debugging):**

```bash
# Terminal 1 — API Gateway
cd api-gateway
mvn spring-boot:run

# Terminal 2 — Document Service
cd document-service
mvn spring-boot:run

# Terminal 3 — Embedding Service
cd embedding-service
mvn spring-boot:run

# Terminal 4 — Query Service
cd query-service
mvn spring-boot:run

# Terminal 5 — Audit Service
cd audit-service
mvn spring-boot:run
```

**Option B — Run all at once (uses background processes):**

```bash
# From backend root
java -jar api-gateway/target/*.jar &
java -jar document-service/target/*.jar &
java -jar embedding-service/target/*.jar &
java -jar query-service/target/*.jar &
java -jar audit-service/target/*.jar &

# View running processes
jobs
```

### Step 5: Verify All Services Are Running

```bash
# All should return {"status":"UP"}
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/actuator/health
curl http://localhost:8084/actuator/health
```

✅ Backend is ready! Now set up the [Frontend](https://github.com/Anandbd09/docintel-ui).

---

## API Endpoints

### Authentication (No JWT required)

```http
POST /api/auth/register
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "SecurePassword123!",
  "name": "John Doe"
}

Response: 200 OK
{
  "id": "userId",
  "email": "user@example.com",
  "token": "eyJhbGc..."
}
```

```http
POST /api/auth/login
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "SecurePassword123!"
}

Response: 200 OK
{
  "token": "eyJhbGc..."
}
```

### Documents (JWT required)

```http
POST /api/documents
Authorization: Bearer {token}
Content-Type: multipart/form-data

{
  "file": (binary),
  "userId": "userId"
}

Response: 202 Accepted
{
  "id": "docId",
  "name": "document.pdf",
  "status": "UPLOADING",
  "progress": "Downloading from S3...",
  "size": 125000,
  "createdOn": "2026-06-03T16:01:50.147"
}
```

```http
GET /api/documents/{userId}
Authorization: Bearer {token}

Response: 200 OK
[
  {
    "id": "docId",
    "name": "document.pdf",
    "status": "READY",
    "progress": "Complete!",
    "tags": ["contract", "legal"],
    "createdOn": "2026-06-03T16:01:50.147"
  }
]
```

```http
GET /api/documents/{userId}/{docId}/status
Authorization: Bearer {token}

Response: 200 OK
{
  "status": "READY",
  "progress": "Embedded 4459 chunks"
}
```

```http
DELETE /api/documents/{userId}/{docId}
Authorization: Bearer {token}

Response: 204 No Content
```

### Queries (JWT required)

```http
POST /api/query
Authorization: Bearer {token}
Content-Type: application/json

{
  "userId": "userId",
  "docId": "docId",
  "question": "What is method overloading?"
}

Response: 200 OK
{
  "answer": "Method overloading is...",
  "sources": [
    {
      "chunk": "Method overloading is a feature where...",
      "similarity": 0.92
    }
  ],
  "cached": false,
  "responseTime": 2350
}
```

```http
POST /api/query/summarize
Authorization: Bearer {token}
Content-Type: application/json

{
  "userId": "userId",
  "docId": "docId"
}

Response: 200 OK
{
  "summary": "## Overview\n\nThis document...",
  "keyTopics": ["topic1", "topic2"],
  "responseTime": 3200
}
```

```http
POST /api/query/search
Authorization: Bearer {token}
Content-Type: application/json

{
  "userId": "userId",
  "question": "Search across all documents"
}

Response: 200 OK
{
  "results": [
    {
      "docId": "docId1",
      "docName": "file1.pdf",
      "answer": "Found in document 1...",
      "similarity": 0.95
    }
  ]
}
```

### Audit Logs (JWT required)

```http
GET /api/audit/{userId}
Authorization: Bearer {token}

Response: 200 OK
[
  {
    "id": "auditId",
    "userId": "userId",
    "question": "What is...",
    "answer": "...",
    "timestamp": "2026-06-03T16:01:50.147",
    "documentsQueried": ["docId1", "docId2"]
  }
]
```

---

## Project Structure

```
doc-intelligence-platform/
├── api-gateway/                    # Spring Cloud Gateway (port 8080)
│   ├── src/main/java/com/docintel/apigateway/
│   │   ├── config/
│   │   │   ├── CorsConfig.java     # CORS + allowed origins
│   │   │   └── JwtAuthFilter.java  # JWT validation
│   │   └── controller/
│   │       └── GatewayController.java
│   ├── application.yml             # Configuration
│   └── pom.xml
│
├── document-service/               # Upload, Auth (port 8081)
│   ├── src/main/java/com/docintel/documentservice/
│   │   ├── controller/
│   │   │   ├── DocumentController.java
│   │   │   └── AuthController.java
│   │   ├── service/
│   │   │   ├── DocumentService.java
│   │   │   └── AuthService.java
│   │   ├── entity/
│   │   │   ├── Document.java
│   │   │   └── User.java
│   │   └── repository/
│   │       ├── DocumentRepository.java
│   │       └── UserRepository.java
│   └── pom.xml
│
├── embedding-service/              # Embeddings (port 8082)
│   ├── src/main/java/com/docintel/embeddingservice/
│   │   ├── service/
│   │   │   ├── EmbeddingService.java
│   │   │   ├── TextExtractionService.java
│   │   │   └── ChromaDBService.java
│   │   ├── kafka/
│   │   │   └── DocumentUploadedConsumer.java
│   │   ├── entity/
│   │   │   └── EmbeddingRequest.java
│   │   └── config/
│   │       └── LangChain4jConfig.java
│   └── pom.xml
│
├── query-service/                  # RAG, LLM (port 8083)
│   ├── src/main/java/com/docintel/queryservice/
│   │   ├── controller/
│   │   │   └── QueryController.java
│   │   ├── service/
│   │   │   ├── QueryService.java
│   │   │   ├── RagService.java
│   │   │   └── LlmService.java
│   │   ├── kafka/
│   │   │   ├── DocumentEmbeddedConsumer.java
│   │   │   └── DocumentDeletedConsumer.java
│   │   └── config/
│   │       ├── LangChain4jConfig.java
│   │       └── RedisConfig.java
│   └── pom.xml
│
├── audit-service/                  # Logging (port 8084)
│   ├── src/main/java/com/docintel/auditservice/
│   │   ├── kafka/
│   │   │   └── QueryExecutedConsumer.java
│   │   ├── entity/
│   │   │   └── AuditLog.java
│   │   └── repository/
│   │       └── AuditLogRepository.java
│   └── pom.xml
│
├── docker-compose.yml              # Infrastructure (Kafka, MongoDB, Redis, ChromaDB)
├── .env                            # Configuration (create this)
├── pom.xml                         # Parent Maven
└── README.md
```

---

## Key Design Decisions

### 1. Kafka for Async Pipeline

**Why:** Embedding takes minutes for large documents. Synchronous REST would block users.

**How:** 
- `doc-uploaded` event → embedding-service processes asynchronously
- User gets instant 202 response
- If embedding-service crashes, message stays in Kafka until recovery

### 2. Per-Document ChromaDB Namespacing

**Why:** Prevent cross-document contamination and ensure per-user data isolation.

**How:** Collection name = `embeddings_{userId}_{docId}`
- Query for user A can only search user A's collections
- Per-document isolation prevents content leakage

### 3. CompletableFuture 5-Thread Pool

**Why:** Sequential embedding takes hours for large documents.

**How:** Parallel embedding with bounded pool
- 10x faster for typical documents
- Bounded to 5 threads prevents resource exhaustion
- Sized for t2.medium's 2 vCPUs

### 4. Redis Caching

**Why:** Query LLM calls take 2+ seconds; cached queries should be instant.

**How:** Cache key = `docId_question`
- First query: ~2.35 seconds
- Cached query: ~11ms (200x improvement)
- TTL: no expiry (manual clear on document delete)

### 5. Environment Variable Routing

**Why:** Single configuration works locally and on EC2.

**How:** 
```yaml
uri: ${DOCUMENT_SERVICE_URL:http://localhost:8081}
```
- Local: uses default `http://localhost:8081`
- EC2: docker-compose sets env var to `http://document-service:8081`

### 6. MongoTemplate Partial Update

**Why:** Prevent overwriting fields set by other services.

**How:** Use MongoTemplate `updateFirst` with `$set` operator
```java
update.set("status", DocumentStatus.READY);
mongoTemplate.updateFirst(query, update, Document.class);
```

### 7. get_or_create for ChromaDB Collections

**Why:** Re-processing documents throws `UniqueConstraintError`.

**How:** Call `get_or_create` before embedding loop
```java
collection = chromadb.get_or_create_collection(name);
```

---

## Troubleshooting

### "Connection refused" to MongoDB

```bash
# Check if container is running
docker ps | grep mongodb

# Start if not running
docker compose -f docker-compose.yml up -d mongodb

# Verify connection
docker exec -it mongodb mongosh
> db.adminCommand("ping")
```

### "Ollama connection refused"

```bash
# Verify Ollama is running
curl http://localhost:11434/api/tags

# If fails, start Ollama
ollama serve

# In another terminal
ollama pull nomic-embed-text
```

### "Kafka broker not available"

```bash
# Check containers
docker compose -f docker-compose.yml ps

# Restart Kafka
docker restart kafka zookeeper

# Verify
docker logs kafka | tail -20
```

### Port already in use

```bash
# Find and kill process using port
# Linux/macOS
lsof -ti:8080 | xargs kill -9
lsof -ti:8081 | xargs kill -9

# Windows (PowerShell)
Get-Process -Id (Get-NetTCPConnection -LocalPort 8080).OwningProcess | Stop-Process
```

### "JWT validation failed"

**Problem:** Token expired or invalid signature  
**Solution:** 
- Ensure `JWT_SECRET` in `.env` is same across all services
- Tokens expire in 24 hours — user must login again
- Check `JwtAuthFilter` is validating correctly

### "Duplicate document" error

**Problem:** Trying to upload same file twice  
**Solution:** 
- MD5 hash checked before S3 upload
- Rename file or delete existing document first
- This is intentional — prevents storage duplication

### "ChromaDB collection not found"

**Problem:** Querying before embedding completes  
**Solution:**
- Wait for document status to be `READY` before querying
- Frontend polls status every 3 seconds
- Check embedding-service logs: `docker logs embedding-service`

### "Out of memory" during embedding

**Problem:** Allocating insufficient Docker/Java memory  
**Solution:**
```bash
# Increase Docker memory limit
docker update --memory 6g embedding-service

# Increase Java heap
export JAVA_OPTS="-Xmx4g"
```

---

## Performance Tuning

### Embedding Pipeline

```yaml
# embedding-service/application.yml
chunking:
  size: 500              # characters per chunk
  overlap: 50            # characters overlap
  
embedding:
  thread-pool-size: 5    # bounded parallelism
  batch-size: 20         # chunks per batch
  
textract:
  auto-detect: true      # use if < 500 chars extracted
  max-file-size: 10485760  # 10MB limit
```

### Query Service

```yaml
# query-service/application.yml
chromadb:
  maxResults: 15         # top N chunks
  similarityThreshold: 0.7  # filter low-quality matches
  
cache:
  ttl: 86400             # 24 hours
  maxSize: 10000         # max cached entries
```

### Database Indexes

```javascript
// MongoDB — create indexes for performance
db.documents.createIndex({ "userId": 1 })
db.documents.createIndex({ "contentHash": 1 })
db.users.createIndex({ "email": 1 }, { unique: true })
db.auditlogs.createIndex({ "userId": 1, "timestamp": -1 })
```

---

## Testing

### Manual Testing with Curl

```bash
# 1. Register
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "Test@1234",
    "name": "Test User"
  }'
# Response: {"token":"eyJhbGc..."}

# 2. Save token
TOKEN="eyJhbGc..."

# 3. Upload document
curl -X POST http://localhost:8080/api/documents \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@document.pdf"

# 4. List documents
curl http://localhost:8080/api/documents/userId \
  -H "Authorization: Bearer $TOKEN"

# 5. Ask question
curl -X POST http://localhost:8080/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "userId",
    "docId": "docId",
    "question": "What is this document about?"
  }'
```

---

## Production Deployment

This local setup is for **development only**. For production:

1. **Database:** AWS RDS (MongoDB) + AWS DocumentDB
2. **Cache:** AWS ElastiCache (Redis)
3. **Message Queue:** AWS MSK (Managed Kafka)
4. **Embeddings:** AWS Bedrock or SageMaker
5. **Orchestration:** EKS (Kubernetes) with auto-scaling
6. **Monitoring:** CloudWatch + Prometheus + Grafana
7. **CDN:** CloudFront or Cloudflare
8. **DNS:** Route 53 or Cloudflare DNS
9. **CI/CD:** GitHub Actions → ECR → EKS

See `DEPLOYMENT.md` for production guide.

---

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Framework | Spring Boot | 3.2.x | Microservices |
| Language | Java | 17+ | Backend |
| Build | Maven | 3.8+ | Build tool |
| Gateway | Spring Cloud Gateway | 4.0.x | API routing |
| Database | MongoDB | 7 | Document storage |
| Cache | Redis | 7 | Query caching |
| Message Queue | Apache Kafka | 7.4 | Async pipeline |
| Vector DB | ChromaDB | 0.4.24 | Embeddings storage |
| Embeddings | Ollama + nomic-embed-text | latest | Local embeddings |
| LLM | Groq LLaMA 3.1 | - | Answer generation |
| RAG Framework | LangChain4j | 0.36.2 | RAG orchestration |
| Cloud Storage | AWS S3 | - | File storage |
| OCR | AWS Textract | - | Scanned PDF support |
| Auth | Spring Security + JWT | - | Authentication |
| Monitoring | Spring Actuator | - | Health checks |

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make changes and test locally
4. Commit: `git commit -am "Add your feature"`
5. Push: `git push origin feature/your-feature`
6. Open a Pull Request

---

## License

MIT License — see LICENSE.md

---

## Support

- **GitHub Issues:** Report bugs and request features
- **Discussions:** Ask questions and share ideas
- **Email:** Contact via GitHub profile

---

## Roadmap

- [ ] Integration tests
- [ ] End-to-end tests
- [ ] Kubernetes deployment
- [ ] Prometheus metrics + Grafana
- [ ] WebSocket real-time progress
- [ ] Document versioning
- [ ] Multilingual embeddings
- [ ] Fine-tuned embeddings
- [ ] User roles and permissions
- [ ] Document sharing

---

## Author

Built by Anand B  
**GitHub:** [@Anandbd09](https://github.com/Anandbd09)
