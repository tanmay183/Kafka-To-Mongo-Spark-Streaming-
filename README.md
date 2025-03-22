### **Project Description: Real-Time E-Commerce Order & Payment Streaming with Kafka and Spark**  

This project implements a **real-time data streaming pipeline** for an **e-commerce platform**, handling **orders and payments** using **Apache Kafka, Apache Spark Structured Streaming, and MongoDB**. The system processes and joins streaming data from **order and payment events** to ensure consistency and real-time analytics.  

---

### **Components & Workflow**  

#### 1. **Order and Payment Data Producers** (`orders_producer.py`, `payments_producer.py`)  
- **Simulates real-time order and payment events** using Kafka Producers.  
- Sends **orders** to Kafka topic **`orders_topic_data_v1`**.  
- Sends **payments** to Kafka topic **`payments_topic_data_v1`**.  
- Introduces **random duplicate orders** to simulate real-world inconsistencies.  

#### 2. **Real-Time Streaming & Stateful Processing** (`join_stream.py`)  
- **Consumes streaming data from Kafka** and processes it using **Apache Spark Structured Streaming**.  
- Reads **order and payment** streams separately and applies **schema validation**.  
- Performs **stateful processing** to **join orders with payments** in real time.  
- **Handles duplicate orders** and **invalid payments** (payments without corresponding orders).  
- Stores the **processed and enriched data in MongoDB** (`ecomm_mart.orders_data_process_fact`).  

---

### **Technologies Used**  
✅ **Apache Kafka** (real-time message streaming).  
✅ **Apache Spark Structured Streaming** (real-time processing & stateful joins).  
✅ **MongoDB** (storing processed order-payment records).  
✅ **Python (pandas, PySpark, Kafka-Python)** for event processing.  

---

### **Outcome**  
🚀 **Real-time ingestion & processing** of orders and payments.  
⚡ **Ensures consistency** by detecting missing or duplicate payments.  
📊 **Stores enriched data** for further analytics & reporting in MongoDB.  

