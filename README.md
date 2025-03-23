# 🚀 Databricks + dbt + tests + CI/CD: The Ultimate Data Pipeline Framework

Welcome to the **Databricks Asset Bundle + dbt** repository! This repo is your one-stop solution for building, testing, and orchestrating scalable data pipelines on Databricks with the power of **dbt (Data Build Tool)**. Say goodbye to messy ETL scripts and hello to modular, version-controlled, and testable data transformations! 🎯

## ✨ What This Repo Does
- **Automates Data Pipelines** using **Databricks Asset Bundles** for seamless deployment and execution.
- **Enables SQL-based Transformations** via **dbt**, making your data modeling clean and efficient.
- **Ensures Data Quality** with **Pytest** & **dbt Testing**, so you can trust your data.
- **Orchestrates & Schedules Workflows** with Databricks Workflows.
- **Supports CI/CD** to promote reliable deployments.

---

## 🚀 Quick Start

### 1️⃣ Clone the Repo
```bash
git clone https://github.com/Maulik-A/DB_ASSET_BUNDLE.git
cd DB_ASSET_BUNDLE
```

### 2️⃣ Install Dependencies
Make sure you have Python and dbt installed. Then, set up your environment:
```bash
pip install -r requirements.txt
```

### 3️⃣ Configure Databricks Asset Bundles
Ensure you have Databricks CLI set up and authenticated. For more check out dabs/DABS_README :
```bash
databricks bundle deploy
```

### 4️⃣ Run dbt Models
Execute dbt transformations inside Databricks:
```bash
dbt run
```

### 5️⃣ Test Your Data
Ensure data integrity with dbt tests:
```bash
dbt test
```

### 6️⃣ Orchestrate with Databricks Workflows
Deploy and trigger Databricks workflows for automated data pipelines:
```bash
databricks workflow run --name sales_data_to_bronze_job
```

---

## 🏗️ Repo Structure
```
📂 DB_ASSET_BUNDLE/
├── 📂 dbt_db /             # dbt models, tests, and macros
│   ├── 📂 models/
│   ├── 📂 tests/
│   ├── 📂 macros/
│   └── dbt_project.yml
├── 📂 dabs /               # Databricks Asset Bundle
│   ├── 📂 resources/
│   │   └── workflow.yml
│   ├── 📂 src/             # Notebooks and Python code
│   │   └── 📂 code/
│   │      ├── python_code.py
│   │      └── validation_notebook.py
│   └── 📂 tests/
│       └── test_code.py    # pytest code
├── 📂 scripts/             # Custom helper scripts
├── 📜 requirements.txt     # Python dependencies
├── 📜 databricks.yml       # Databricks Asset Bundle configurations
└── 📜 README.md            # This file!
```

---

## 🔥 Why This Repo?
✅ **Scalable & Modular** – Build data pipelines that grow with your business.  
✅ **CI/CD-Ready** – Integrates seamlessly with GitHub Actions or Azure DevOps.  
✅ **Version-Controlled Data Models** – Treat your SQL transformations like code.  
✅ **Automated Testing** – Validate data with dbt tests before deployment.  
✅ **Cloud-Native & Cost-Efficient** – Harness Databricks’ compute power efficiently.  

---

## 🛠️ Useful Commands
| Task                          | Command |
|-------------------------------|---------|
| Deploy Databricks Bundle      | `databricks bundle deploy` |
| Run a dbt Model               | `dbt run` |
| Run dbt Tests                 | `dbt test` |
| List Databricks Workflows     | `databricks workflow list` |
| Trigger a Databricks Workflow | `databricks workflow run --name sales_data_to_bronze_job` |

---

## 🚀 Contribute & Get Involved
We welcome contributions! Feel free to open issues, submit PRs, or suggest improvements. Let’s build the future of data together! 🛠️

---

## 📜 License
This project is licensed under the MIT License.

Happy coding! 🚀

