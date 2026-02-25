# Data Vault 2.0 Tax System Data Warehouse Using Medallion Architecture

[![SQL Server](https://img.shields.io/badge/SQL%20Server-2025-CC2927?logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![SSIS](https://img.shields.io/badge/SSIS-2022-5C2D91?logo=visualstudio&logoColor=white)](https://learn.microsoft.com/en-us/sql/integration-services/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **Master's Thesis Implementation** — A complete Data Vault 2.0 data warehouse with medallion architecture (Staging → Bronze → Silver → Gold) for Cambodia's tax administration system, built on SQL Server 2025 and SSIS 2022.

---

## 📋 Overview

This repository contains the full implementation of a proof-of-concept data warehouse for the **General Department of Taxation (GDT), Cambodia**, using **Data Vault 2.0** methodology combined with a **four-layer medallion architecture**. The project demonstrates how modern data warehousing patterns can be applied to government tax administration systems.

### Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   TaxSystemDB   │────▶│  DV_Staging   │────▶│  DV_Bronze   │────▶│  DV_Silver   │────▶│  DV_Gold     │
│   (Source)      │     │  (Landing)    │     │  (Raw Vault) │     │  (Business   │     │  (Star       │
│                 │     │              │     │  Hub/Sat/Link │     │   Vault)     │     │   Schema)    │
│  9 Tables       │     │  9 STG Tables │     │  9H/10S/5L   │     │  3 PIT/1 BRG │     │  7 DIM/4 FCT │
└─────────────────┘     └──────────────┘     └──────────────┘     │  2 BUS       │     └──────────────┘
                                                                   └──────────────┘
                         ◄──────────── ETL_Control (Logging, Watermarks, Error Handling) ──────────────►
```

### Key Metrics

| Metric | Value |
|--------|-------|
| Full Load | 336,677 records in 120.9s (2,785 rec/s) |
| Incremental Load | 206,780 records in 93.6s (2,209 rec/s) |
| Query Speedup (Gold vs Raw Vault) | 1.03× – 8.3× |
| Total Database Objects | 50 tables across 6 databases |
| SSIS Packages | 49 child packages + 3 master packages |
| Stored Procedures | 15 orchestration procedures |

---

## 📁 Repository Structure

```
dv2-tax-data-warehouse-using-medallion/
│
├── sql-scripts/                          # Core SQL implementation (execute in order)
│   ├── 00_CleanAll_FreshFullLoad.sql      # Reset all databases for fresh start
│   ├── 01_CreateDatabaseStructure.sql     # Source database (TaxSystemDB) DDL
│   ├── 02_TransactionData.sql             # Simulated tax data (1,000 taxpayers)
│   ├── 03_ETL_Control_Setup.sql           # ETL Control framework (logging, watermarks)
│   ├── 04_DDL_Architecture_DW.sql         # Data warehouse DDL (Staging/Bronze/Silver/Gold)
│   ├── 05_ETL_Staging_Procedures.sql      # Staging layer helper procedures
│   ├── 06_ETL_Bronze_Procedures.sql       # Bronze layer (Hub/Satellite/Link) procedures
│   ├── 07_ETL_Silver_Procedures.sql       # Silver layer (PIT/Bridge/Business) procedures
│   ├── 08_ETL_Gold_Procedures.sql         # Gold layer (Dimension/Fact) procedures
│   └── 09_ETL_Master_Orchestration.sql    # Master ETL (Full + Incremental load)
│
├── verification/                          # Testing and validation scripts
│   ├── 10_Verify_FullLoad.sql             # Full load verification queries
│   ├── 11_Verify_IncrementalLoad.sql      # Incremental load verification
│   ├── 11_Verify_IncrementalLoad_Clean.sql # Clean verification for screenshots
│   └── 12_IncrementalTest_SourceChanges.sql # Delta test data for incremental loads
│
├── benchmarks/                            # Performance benchmarking
│   ├── Benchmarks_Verification.sql        # Benchmark queries (ETL + query performance)
│   └── Benchmarks_Verification.docx       # Results with SSMS screenshots
│
├── ssis-guide/                            # SSIS package implementation guide
│   └── Technical_Implementation_Guide.docx # Step-by-step SSIS build instructions
│
├── docs/                                  # Thesis documents
│   ├── Final_Report.docx                  # Complete thesis report (6 chapters)
│   ├── Final_Presentation.pptx            # Defense presentation (18 slides)
│   ├── Thesis_Defense_Preparation.docx    # Q&A preparation (18 questions)
│   └── Deployment_Operations_Guide.docx   # GCP VM deployment guide
│
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- **SQL Server 2025** (Developer or Enterprise Edition)
- **SQL Server Management Studio (SSMS)** 20+
- **Visual Studio 2022** with SQL Server Integration Services (SSIS) extension
- Minimum 8 GB RAM, 20 GB free disk space

### Installation

Execute the SQL scripts in numerical order against your SQL Server instance:

```bash
# Step 1: Create source database and load test data
sqlcmd -S localhost -i sql-scripts/01_CreateDatabaseStructure.sql
sqlcmd -S localhost -i sql-scripts/02_TransactionData.sql

# Step 2: Create ETL control framework
sqlcmd -S localhost -i sql-scripts/03_ETL_Control_Setup.sql

# Step 3: Create data warehouse databases (Staging, Bronze, Silver, Gold)
sqlcmd -S localhost -i sql-scripts/04_DDL_Architecture_DW.sql

# Step 4: Create stored procedures for all layers
sqlcmd -S localhost -i sql-scripts/05_ETL_Staging_Procedures.sql
sqlcmd -S localhost -i sql-scripts/06_ETL_Bronze_Procedures.sql
sqlcmd -S localhost -i sql-scripts/07_ETL_Silver_Procedures.sql
sqlcmd -S localhost -i sql-scripts/08_ETL_Gold_Procedures.sql

# Step 5: Create master orchestration procedures
sqlcmd -S localhost -i sql-scripts/09_ETL_Master_Orchestration.sql
```

### Running the ETL Pipeline

```sql
-- Full Load (first-time load of all data)
USE ETL_Control;
EXEC dbo.usp_MasterETL_FullLoad;

-- Incremental Load (delta changes only)
-- First, apply test changes:
--   Run: verification/12_IncrementalTest_SourceChanges.sql
-- Then:
EXEC dbo.usp_MasterETL_IncrementalLoad;
```

### Verifying Results

```sql
-- After Full Load
-- Run: verification/10_Verify_FullLoad.sql

-- After Incremental Load
-- Run: verification/11_Verify_IncrementalLoad.sql

-- Performance Benchmarks
-- Run: benchmarks/Benchmarks_Verification.sql
```

---

## 🏗️ Architecture Details

### Six Databases

| Database | Purpose | Key Objects |
|----------|---------|-------------|
| **TaxSystemDB** | Source system (simulated) | 9 tables (3 lookup + 3 reference + 3 transaction) |
| **ETL_Control** | Pipeline orchestration | BatchLog, StepLog, ErrorLog, Watermark, Configuration |
| **DV_Staging** | Landing zone | 9 staging tables (truncate & reload / watermark delta) |
| **DV_Bronze** | Raw Data Vault | 9 Hubs, 10 Satellites, 5 Links |
| **DV_Silver** | Business Vault | 3 PIT tables, 1 Bridge, 2 Business Vault tables |
| **DV_Gold** | Star Schema | 7 Dimensions (5 SCD1 + 2 SCD2), 4 Fact tables |

### Data Vault 2.0 Pattern

- **Hubs**: Business keys with SHA2_256 hash keys (HUB_Taxpayer, HUB_Declaration, etc.)
- **Satellites**: Descriptive attributes with HashDiff change detection and end-dating
- **Links**: Many-to-many relationships (LNK_TaxpayerDeclaration, LNK_DeclarationPayment, etc.)
- **PIT Tables**: Point-in-Time snapshots for efficient temporal queries
- **Bridge Table**: Pre-joined Taxpayer-Owner relationships

### ETL Control Framework

- **Batch-level logging**: Every ETL run tracked with BatchID, status, record counts
- **Step-level logging**: Each of 49 steps individually tracked with TRY-CATCH
- **Watermark-based incremental**: Per-table watermarks for delta extraction
- **Error handling**: Centralized error logging with severity classification
- **Configuration table**: Runtime-configurable parameters (retry attempts, thresholds)

### SSIS Package Hierarchy

```
Master_Complete_Pipeline.dtsx
├── [FULL] → Master_Full_Load.dtsx
│   ├── STG_Full_Load_All.dtsx → 9 staging child packages
│   ├── BRZ_Load_All_Hubs.dtsx → 9 hub child packages
│   ├── BRZ_Load_All_Satellites.dtsx → 9 satellite child packages (+ 1 risk level)
│   ├── BRZ_Load_All_Links.dtsx → 5 link child packages
│   ├── SLV_Load_All.dtsx → 6 silver child packages
│   ├── GLD_Load_All_Dimensions.dtsx → 7 dimension child packages
│   └── GLD_Load_All_Facts.dtsx → 4 fact child packages
│
└── [INCREMENTAL] → Master_Incremental_Load.dtsx
    └── (same structure, watermark-based delta extraction)
```

> **Note**: SSIS `.dtsx` package files are not included in this repository. The `ssis-guide/Technical_Implementation_Guide.docx` provides complete step-by-step instructions to build all 49+ packages in Visual Studio, including every variable, expression, parameter binding, data flow component configuration, and OnError event handler.

---

## 📊 Performance Benchmarks

### ETL Load Performance

| Metric | Full Load | Incremental Load |
|--------|-----------|------------------|
| **Total Records** | 336,677 | 206,780 |
| **Total Duration** | 120.9s | 93.6s |
| **Throughput** | 2,785 rec/s | 2,209 rec/s |
| Staging Layer | 22.8s (58,813 records) | 6.0s (58,824 records) |
| Bronze Layer | 39.5s (184,108 records) | 30.3s (41 records) |
| Silver Layer | 3.4s (2,004 records) | 3.5s (57,186 records) |
| Gold Layer | 11.0s (91,752 records) | 12.7s (90,729 records) |

### Query Performance (Gold Star Schema vs Raw Vault)

| Query Complexity | Gold (ms) | Raw Vault (ms) | Speedup |
|-----------------|-----------|----------------|---------|
| Simple (single taxpayer lookup) | 24 | 199 | **8.3×** |
| Medium (category aggregation) | 18 | 123 | **6.8×** |
| Complex (full cross-tab, 38K rows) | 771 | 798 | **1.03×** |

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [Final Report](docs/Final_Report.docx) | Complete 6-chapter thesis (~80 pages) |
| [Final Presentation](docs/Final_Presentation.pptx) | 18-slide defense presentation with speaker notes |
| [Technical Implementation Guide](ssis-guide/Technical_Implementation_Guide.docx) | Step-by-step SSIS package build guide (~150 pages) |
| [Deployment Guide](docs/Deployment_Operations_Guide.docx) | Google Cloud Platform VM deployment |
| [Benchmark Results](benchmarks/Benchmarks_Verification.docx) | SSMS screenshots of all benchmark runs |
| [Defense Preparation](docs/Thesis_Defense_Preparation.docx) | 18 potential Q&A for thesis defense |

---

## 🔧 Technology Stack

- **Database**: SQL Server 2025 Developer Edition (v17, RTM 17.0.1000.7)
- **ETL**: SQL Server Integration Services (SSIS) 2022
- **IDE**: Visual Studio 2022 Community + SSIS Extension
- **OS**: Windows Server 2025 Datacenter (GCP VM)
- **Methodology**: Data Vault 2.0 (Linstedt)
- **Architecture**: Medallion (Staging → Bronze → Silver → Gold)
- **Deployment**: Google Cloud Platform VM (e2-standard-4, 4 vCPU / 16 GB RAM)
- **Hash Algorithm**: SHA2_256 for Hub keys and Satellite HashDiff

---

## 📖 References

This implementation is based on:

- Linstedt, D. & Olschimke, M. (2016). *Building a Scalable Data Warehouse with Data Vault 2.0*
- Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit*, 3rd Edition
- Inmon, W.H. (2005). *Building the Data Warehouse*, 4th Edition

Full reference list (34 citations) available in the [Final Report](docs/Final_Report.docx).

---

## 👤 Author

**Mr. Sot So**
- Chief of Data Management Bureau, General Department of Taxation, Cambodia
- Master of Engineering in Data Science and Engineering
- Royal University of Phnom Penh

**Supervisor**: Mr. Chap Chanpiseth

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## ⚠️ Disclaimer

This is an academic proof-of-concept implementation. The tax data is **simulated** and does not contain any real taxpayer information. This project is a personal master's degree research project and is not affiliated with the General Department of Taxation's production systems.
