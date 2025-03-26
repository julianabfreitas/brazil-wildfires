#!/bin/bash

# Nome da rede
NETWORK_NAME="brazil-wildfires-network"

# Verifica e cria a rede, se necessário
if docker network ls | grep -q "$NETWORK_NAME"; then
  echo "A rede '$NETWORK_NAME' já existe. Nenhuma ação necessária."
else
  echo "A rede '$NETWORK_NAME' não foi encontrada. Criando a rede..."
  docker network create "$NETWORK_NAME"
  if [ $? -eq 0 ]; then
    echo "Rede '$NETWORK_NAME' criada com sucesso!"
  else
    echo "Erro ao criar a rede '$NETWORK_NAME'. Verifique o Docker e tente novamente."
    exit 1
  fi
fi