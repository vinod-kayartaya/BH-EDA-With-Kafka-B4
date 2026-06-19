# Overview of Kafka Security

This module focuses on securing Kafka clusters and ensuring that only authorized producers, consumers, applications, and administrators can access Kafka resources.

In production environments, Kafka often carries sensitive business data such as payments, customer information, banking transactions, audit logs, and fraud detection events. Security becomes a critical requirement. 



# Why Kafka Security Matters

Consider a payment processing platform.

Events may contain:

```json
{
  "paymentId":"P1001",
  "customerId":"C100",
  "amount":25000,
  "cardNumber":"XXXX-XXXX-XXXX-1234"
}
```

Without security:

* Anyone can consume payment data
* Fake producers can inject transactions
* Attackers can delete topics
* Sensitive information can be intercepted

Kafka security protects:

* Confidentiality
* Integrity
* Authentication
* Authorization


# Kafka Security Layers

Kafka security consists of three layers:

## Layer 1: Encryption

Protects data while traveling through the network.

```text
Producer
    |
    | Encrypted
    |
Kafka Broker
    |
    | Encrypted
    |
Consumer
```

Implemented using:

* TLS
* SSL Certificates


## Layer 2: Authentication

Verifies identity.

Questions answered:

```text
Who are you?
```

Examples:

* Application A
* Payment Service
* Fraud Service
* Administrator

Implemented using:

* SASL
* SSL Client Certificates


## Layer 3: Authorization

Determines permissions.

Questions answered:

```text
What are you allowed to do?
```

Examples:

| User            | Permission     |
| --------------- | -------------- |
| Payment Service | Write payments |
| Fraud Service   | Read payments  |
| Admin           | Create topics  |
| Analyst         | Read reports   |

Implemented using:

* ACLs


# Security Architecture Overview

```text
             +----------------+
             |   Producer     |
             +--------+-------+
                      |
                 TLS/SSL
                      |
                      v

+--------------------------------------+
|          Kafka Cluster               |
|                                      |
|  Authentication (SASL/SSL)          |
|  Authorization (ACL)                |
+----------------+---------------------+
                 |
              TLS/SSL
                 |
                 v

         +--------------+
         |  Consumer    |
         +--------------+
```


# Part 1: TLS / SSL Basics


# What is SSL?

SSL stands for:

```text
Secure Socket Layer
```

Modern Kafka uses:

```text
TLS (Transport Layer Security)
```

The terms SSL and TLS are often used interchangeably.


# Problem Without SSL

Normal communication:

```text
Producer
    |
    | payment=25000
    |
Broker
```

An attacker can intercept packets.

```text
Producer
     |
     | payment=25000
     |
  Hacker
     |
Broker
```

Sensitive information becomes visible.


# Communication With SSL

```text
Producer
    |
    | Encrypted Data
    |
Broker
```

Captured packets become unreadable.


# SSL Certificates

SSL relies on certificates.

Think of a certificate as:

```text
Digital Identity Card
```

Example:

```text
Broker Identity

Name: kafka-broker-1
Issued By: Company CA
Valid Until: 2030
```


# Certificate Components

A certificate contains:

* Public Key
* Owner Information
* Expiry Date
* Issuer Details
* Digital Signature


# SSL Handshake

When a producer connects:

```text
Producer ---> Broker

1. Hello
2. Broker sends certificate
3. Producer verifies certificate
4. Encryption keys exchanged
5. Secure communication starts
```


# One-Way SSL

Only broker proves identity.

```text
Producer ---> Broker
```

Common deployment.


# Mutual SSL (mTLS)

Both sides prove identity.

```text
Producer <----> Broker
```

Used in highly secure environments.


# Kafka SSL Configuration

Broker configuration:

```properties
listeners=SSL://localhost:9093

ssl.keystore.location=/certs/server.keystore.jks
ssl.keystore.password=password

ssl.truststore.location=/certs/server.truststore.jks
ssl.truststore.password=password
```


# Producer SSL Configuration

```properties
security.protocol=SSL

ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
```


# Consumer SSL Configuration

```properties
security.protocol=SSL

ssl.truststore.location=client.truststore.jks
ssl.truststore.password=password
```


# SSL Advantages

* Encrypts data
* Prevents packet sniffing
* Protects credentials
* Prevents man-in-the-middle attacks


# Part 2: SASL Authentication


# What is SASL?

SASL stands for:

```text
Simple Authentication and Security Layer
```

Used to verify identities.


# Authentication Analogy

Entering an office building:

```text
Guard: Who are you?
Employee: ID Card
Guard: Access granted
```

Kafka works similarly.


# SASL Workflow

```text
Producer
    |
    | Username/Password
    |
Broker
```

Broker verifies identity.


# Supported SASL Mechanisms

Kafka supports:

| Mechanism   | Usage                   |
| ----------- | ----------------------- |
| PLAIN       | Username/password       |
| SCRAM       | Secure password storage |
| GSSAPI      | Kerberos                |
| OAUTHBEARER | OAuth tokens            |


# SASL PLAIN

Simplest method.

```text
Username: payment-service
Password: secret123
```

Configuration:

```properties
security.protocol=SASL_SSL

sasl.mechanism=PLAIN
```


# SCRAM Authentication

Recommended over PLAIN.

Benefits:

* Password hashing
* Better protection
* Industry standard

Example:

```properties
sasl.mechanism=SCRAM-SHA-256
```

or

```properties
sasl.mechanism=SCRAM-SHA-512
```


# Producer Example

```properties
security.protocol=SASL_SSL

sasl.mechanism=SCRAM-SHA-256

sasl.jaas.config=
org.apache.kafka.common.security.scram.ScramLoginModule required
username="payment-service"
password="secret";
```


# Consumer Example

```properties
security.protocol=SASL_SSL

sasl.mechanism=SCRAM-SHA-256

sasl.jaas.config=
org.apache.kafka.common.security.scram.ScramLoginModule required
username="fraud-service"
password="secret";
```


# Why SASL_SSL is Popular

Combines:

```text
Authentication + Encryption
```

```text
SASL = Identity Verification
SSL  = Secure Communication
```

Most enterprise deployments use:

```text
SASL_SSL
```


# Part 3: Authorization Using ACLs


# What is Authorization?

After authentication succeeds:

```text
Who are you?
```

Next question:

```text
What are you allowed to do?
```


# What is an ACL?

ACL stands for:

```text
Access Control List
```

Defines permissions.


# ACL Example

```text
User: payment-service

Can:
  WRITE payments

Cannot:
  DELETE payments
```


# Kafka Resources

ACLs can protect:

* Topics
* Consumer Groups
* Clusters
* Transaction IDs


# Common Operations

| Operation | Meaning       |
| --------- | ------------- |
| READ      | Consume       |
| WRITE     | Produce       |
| CREATE    | Create Topic  |
| DELETE    | Delete Topic  |
| ALTER     | Modify        |
| DESCRIBE  | View Metadata |


# Example Security Design

Topics:

```text
payments
fraud-alerts
audit-events
```

Users:

```text
payment-service
fraud-service
admin
```

Permissions:

| User            | Permission         |
| --------------- | ------------------ |
| payment-service | WRITE payments     |
| fraud-service   | READ payments      |
| fraud-service   | WRITE fraud-alerts |
| admin           | ALL                |


# Creating ACLs

Grant producer access:

```bash
kafka-acls.sh \
--add \
--allow-principal User:payment-service \
--operation WRITE \
--topic payments
```


# Consumer ACL

```bash
kafka-acls.sh \
--add \
--allow-principal User:fraud-service \
--operation READ \
--topic payments
```


# Admin ACL

```bash
kafka-acls.sh \
--add \
--allow-principal User:admin \
--operation ALL \
--cluster
```


# Viewing ACLs

```bash
kafka-acls.sh --list
```


# Principle of Least Privilege

Always grant minimum permissions.

Bad:

```text
Everyone = ALL permissions
```

Good:

```text
Payment Service = WRITE only
Fraud Service   = READ only
```


# Enterprise Security Architecture

```text
                    +----------------+
                    |   Admin User   |
                    +--------+-------+
                             |
                         ALL ACL
                             |
                             v

+------------------------------------------------+
|                Kafka Cluster                   |
+------------------------------------------------+
|                                                |
| Topic: payments                                |
| Topic: fraud-alerts                            |
| Topic: audit-events                            |
|                                                |
+------------------------------------------------+
       ^                        ^
       |                        |
 WRITE payments         READ payments
       |                        |
       |                        |
+------+-----+           +------+------+
| PaymentSvc |           | FraudSvc    |
+------------+           +-------------+
```


# Security Best Practices

## 1. Always Enable Encryption

Avoid:

```text
PLAINTEXT
```

Use:

```text
SSL
or
SASL_SSL
```


## 2. Use SCRAM Instead of PLAIN

Preferred:

```text
SCRAM-SHA-256
SCRAM-SHA-512
```


## 3. Enable ACLs

Never allow unrestricted access.


## 4. Separate Service Accounts

Avoid:

```text
admin/admin
```

Use:

```text
payment-service
fraud-service
analytics-service
```


## 5. Rotate Credentials

Regularly update:

* Passwords
* Certificates
* Secrets


## 6. Audit Access

Monitor:

* Failed logins
* Unauthorized access
* ACL changes


# Real-World Payment Processing Example

Our training system contains:

### Producer Services

* Payment Service
* Merchant Service
* Refund Service

### Consumers

* Fraud Detection
* Analytics Engine
* Reporting System

Security configuration:

```text
TLS Encryption
SASL Authentication
ACL Authorization
```

Benefits:

* Secure payment data
* Verified producers
* Controlled access
* Regulatory compliance


# Hands-On Lab

## Lab Objective

Secure a Kafka cluster using SASL and ACLs.


### Step 1

Create users:

```text
payment-service
fraud-service
```


### Step 2

Enable SCRAM authentication.


### Step 3

Configure producer credentials.


### Step 4

Configure consumer credentials.


### Step 5

Create ACLs:

```text
payment-service → WRITE payments
fraud-service → READ payments
```


### Step 6

Verify behavior:

Allowed:

```text
payment-service writes payments
fraud-service reads payments
```

Blocked:

```text
fraud-service deletes topic
```


# Module Summary

In this module you learned:

* Kafka security architecture
* TLS/SSL encryption
* SSL certificates and handshakes
* SASL authentication
* SCRAM and PLAIN mechanisms
* ACL-based authorization
* Enterprise security practices
* Securing payment processing systems

These concepts form the foundation of production-grade Kafka deployments and are essential for any organization handling sensitive or regulated data. 
