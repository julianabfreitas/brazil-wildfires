-- Conecte-se ao banco principal
\c brazil-wildfires-db;

-- Remova os schemas desnecessários
DROP SCHEMA IF EXISTS tiger CASCADE;
DROP SCHEMA IF EXISTS tiger_data CASCADE;
DROP SCHEMA IF EXISTS topology CASCADE;