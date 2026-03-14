# Microsoft Purview Data Governance 🚀

Automating Microsoft Purview data governance operations using **REST APIs** and **Python SDK**.  
---

## 📖 Overview
Data governance is critical for ensuring trust, compliance, and discoverability in modern data platforms.  
This project shows how to:
- Deploy Microsoft Purview account programmatically
- Connect to Microsoft Purview programmatically
- Export and restore Data Map's collections from Default Domain
- CRUD - collections, data sources
- Export and restore Data Source details
- Export Metadata By Source Type
- Scale governance tasks with Python automation

---

## 🛠️ Tech Stack
- **Microsoft Purview REST API**
- **Python SDK for Purview**
- **Azure Active Directory (for authentication)**

---

## Features
- **Create Purview Account**
- **Data Map**
  
![Alt text](https://github.com/UmaMahesh-Devarajula/Microsoft-Purview-Data-Governanace-Utility-Kit/blob/5a9a5c081e11cf312670644e02486682888317d5/datasources/Screenshot%202026-03-14%20182240.png)
-  **Collections**
    - **Create or Update Collection**
    - **Delete Collection**
    - **List Collections**
    - **Export Collections info into CSV**
    - **Restore Collections from CSV**
      
- **Data Sources**
  - **Register a Data Source**
  - **Delete a Data Sources**
  - **List Data Sources**
  - **Export DataSources to a JSON file**
  - **Restore DataSources from JSON file**
 
- **Export Metadata by Source type**
![Alt text](https://github.com/UmaMahesh-Devarajula/Microsoft-Purview-Data-Governanace-Utility-Kit/blob/8f291fa11216d87af73fdf6f966738d4b32c6927/Metadata/exportmetadatabysourcetype.PNG)
     
## 🚀 Getting Started

### Prerequisites
- Azure subscription with Microsoft Purview account
- Azure AD app registration (client ID, tenant ID, secret)
- Python 3.9+ environment

### Installation
```bash
git clone https://github.com/UmaMahesh-Devarajula/Microsoft-Purview-Data-Governanace-Utility-Kit.git
cd Microsoft-Purview-Data-Governanace-Utility-Kit
py -m pip install -r requirements.txt
purview.py
```
### **Menu**
<img width="565" height="134" alt="image" src="https://github.com/user-attachments/assets/c2e8abd8-84c1-4ac2-a8aa-2152544c3b94" />

