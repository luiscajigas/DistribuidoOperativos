# Plataforma de Votación Zero Trust (Kafka + RabbitMQ + JWT)

Stack: Node.js + TypeScript en todos los microservicios. Kafka: 1 broker KRaft. RabbitMQ: OAuth2. TLS interno con certificados self-signed. Despliegue en 6 EC2 (misma VPC privada; solo `vm-api` con IP pública).

## VMs y roles

- `vm-auth`: `auth_server` (emite JWT y expone JWKS)
- `vm-brokers`: Kafka + RabbitMQ (Docker runtime)
- `vm-api`: `voting_api` (DMZ)
- `vm-init`: `voter_registration_service` (one-shot)
- `vm-core`: servicios de procesamiento (stateful + RPC)
- `vm-app`: dashboards/workers

## Puertos esperados (red Zero Trust)

- `vm-auth`: 443/tcp
- `vm-brokers`: 9092/tcp (Kafka SASL_SSL + OAUTHBEARER), 5672/tcp (RabbitMQ TLS + OAuth2)
- `vm-api`: 80/tcp, 443/tcp (entrada pública)

## Autenticación

- Los servicios obtienen un JWT en `https://<VM_AUTH_IP>/token` con `client_id` y `client_secret`.
- Kafka valida el JWT usando `sasl.oauthbearer.jwks.endpoint.url` apuntando al JWKS del `auth_server`.
- RabbitMQ valida el JWT con `rabbitmq_auth_backend_oauth2`. El cliente pasa el token como password; el campo username se ignora según la documentación de RabbitMQ OAuth2.

## Estructura del repo

- `apps/`: microservicios (Node+TS)
- `packages/`: librerías compartidas
- `infra/ansible/`: aprovisionamiento y despliegue en AWS EC2

## Ejecución local (dev)

Requiere Node 20+ (probado con Node 24).

```bash
npm install
npm run build
```
