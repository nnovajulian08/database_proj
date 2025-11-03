-- ===============================================================
-- ELECTROGRID NORMALIZED DATABASE STRUCTURE (PostgreSQL)
-- Author: Julian Sebastián Núñez Nova
-- Purpose: Fully normalized database schema based on relational model
-- Safe to re-run (drops all existing tables before recreation)
-- ===============================================================

-- Optional schema
-- CREATE SCHEMA IF NOT EXISTS electrogrid;
-- SET search_path TO electrogrid;

-- ===============================================================
-- 1️⃣ DROP TABLES (in reverse dependency order)
-- ===============================================================

DROP TABLE IF EXISTS Technician_Skill CASCADE;
DROP TABLE IF EXISTS Bills CASCADE;
DROP TABLE IF EXISTS Service_Orders CASCADE;
DROP TABLE IF EXISTS Connections CASCADE;
DROP TABLE IF EXISTS Technician CASCADE;
DROP TABLE IF EXISTS Client CASCADE;
DROP TABLE IF EXISTS Person CASCADE;
DROP TABLE IF EXISTS Skills CASCADE;
DROP TABLE IF EXISTS Region CASCADE;
DROP TABLE IF EXISTS Connection_Type CASCADE;
DROP TABLE IF EXISTS Status CASCADE;
DROP TABLE IF EXISTS Service_Type CASCADE;

-- ===============================================================
-- 2️⃣ LOOKUP TABLES
-- ===============================================================

CREATE TABLE Region (
    region_name VARCHAR(100) PRIMARY KEY
);

CREATE TABLE Connection_Type (
    connection_type VARCHAR(50) PRIMARY KEY
);

CREATE TABLE Status (
    status VARCHAR(50) PRIMARY KEY
);

CREATE TABLE Service_Type (
    service_type VARCHAR(50) PRIMARY KEY
);

-- ===============================================================
-- 3️⃣ CORE ENTITIES
-- ===============================================================

CREATE TABLE Person (
    person_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(150) UNIQUE,
    phone VARCHAR(50) UNIQUE
);

CREATE TABLE Client (
    person_id VARCHAR(50) PRIMARY KEY REFERENCES Person(person_id) ON DELETE CASCADE,
    address TEXT NOT NULL
);

CREATE TABLE Technician (
    person_id VARCHAR(50) PRIMARY KEY REFERENCES Person(person_id) ON DELETE CASCADE,
    region_name VARCHAR(100) REFERENCES Region(region_name)
);

CREATE TABLE Skills (
    skill_name VARCHAR(100) PRIMARY KEY
);

-- ===============================================================
-- 4️⃣ RELATIONSHIP TABLES (M:N and dependent entities)
-- ===============================================================

CREATE TABLE Technician_Skill (
    technician_id VARCHAR(50) REFERENCES Technician(person_id) ON DELETE CASCADE,
    skill_name VARCHAR(100) REFERENCES Skills(skill_name) ON DELETE CASCADE,
    PRIMARY KEY (technician_id, skill_name)
);

CREATE TABLE Connections (
    connection_id VARCHAR(50) PRIMARY KEY,
    property_address TEXT NOT NULL,
    install_date DATE,
    meter_serial VARCHAR(100),
    connection_type VARCHAR(50) REFERENCES Connection_Type(connection_type),
    status VARCHAR(50) REFERENCES Status(status),
    client_id VARCHAR(50) REFERENCES Client(person_id) ON DELETE CASCADE,
    technician_id VARCHAR(50) REFERENCES Technician(person_id)
);

CREATE TABLE Bills (
    bills_id VARCHAR(50) PRIMARY KEY,
    period_starts DATE,
    period_ends DATE,
    kwh_used NUMERIC,
    amount NUMERIC(10,2),
    issue_date DATE,
    payment_date DATE,
    client_id VARCHAR(50) REFERENCES Client(person_id),
    connection_id VARCHAR(50) REFERENCES Connections(connection_id)
);

CREATE TABLE Service_Orders (
    service_order_id VARCHAR(50) PRIMARY KEY,
    service_type VARCHAR(50) REFERENCES Service_Type(service_type),
    start_date DATE,
    end_date DATE,
    notes TEXT,
    client_id VARCHAR(50) REFERENCES Client(person_id),
    technician_id VARCHAR(50) REFERENCES Technician(person_id),
    connection_id VARCHAR(50) REFERENCES Connections(connection_id)
);

-- ===============================================================
-- ✅ DONE
-- ===============================================================

-- You can test by listing tables:
-- \dt
