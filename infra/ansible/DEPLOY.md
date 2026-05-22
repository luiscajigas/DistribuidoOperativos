# Despliegue en AWS (Ansible + Zero Trust)

## 1) Inventario

1. Copia `inventory.example.ini` a `inventory.ini`.
2. Ajusta:
   - `ansible_ssh_private_key_file`
   - `admin_cidr` (tu IP pública en /32)
   - `repo_url` y `repo_ref`
   - `ansible_host` y `private_ip` de las 6 VMs

## 2) Secretos (mínimo)

En `infra/ansible/site.yml` cambia los valores `change-me` de:

- `clients_json` (vm-auth)
- `voting_api_client_secret`
- `voter_registration_client_secret`
- `user_validation_client_secret`

## 3) Ejecutar playbook

Desde tu máquina:

```bash
cd infra/ansible
ansible-playbook -i inventory.ini site.yml
```

## 4) Verificación en VMs

### nftables

```bash
sudo nft list ruleset
sudo nft -c -f /etc/nftables.conf
sudo systemctl status nftables
```

### auth_server (vm-auth)

```bash
curl -k https://127.0.0.1/health
curl -k https://127.0.0.1/.well-known/jwks.json
```

### Kafka y RabbitMQ (vm-brokers)

```bash
sudo systemctl status kafka
sudo systemctl status rabbitmq
docker ps
```

### voting_api (vm-api)

```bash
sudo systemctl status voting-api
curl -k https://127.0.0.1/health
```

