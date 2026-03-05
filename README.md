# Data Vault 2.0 Tax System Data Warehouse Using Medallion Architecture

[![SQL Server](https://img.shields.io/badge/SQL%20Server-2025-CC2927?logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![SSIS](https://img.shields.io/badge/SSIS-2022-5C2D91?logo=visualstudio&logoColor=white)](https://learn.microsoft.com/en-us/sql/integration-services/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **Master's Thesis** — A complete Data Vault 2.0 data warehouse with medallion architecture (Staging → Bronze → Silver → Gold) for Cambodia's tax administration system, built on SQL Server 2025 and SSIS 2022.

**Author:** Mr. Sot So — Master of Science in Data Science and Engineering, Royal University of Phnom Penh (February 2026)
**Supervisor:** Mr. Chap Chanpiseth

---

## Overview

This repository contains the full implementation of a proof-of-concept data warehouse for the **General Department of Taxation (GDT), Cambodia**, using **Data Vault 2.0** methodology organized within a **four-layer medallion architecture**. The project demonstrates how modern data warehousing patterns address key challenges in government tax administration: schema rigidity, incomplete historical tracking, fragile ETL pipelines, and the gap between raw data and business-ready analytics.

### Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   TaxSystemDB   │────▶│  DV_Staging   │────▶│  DV_Bronze   │────▶│  DV_Silver   │────▶│   DV_Gold    │
│   (Source)      │     │  (Landing)    │     │  (Raw Vault) │     │  (Business   │     │  (Star       │
│                 │     │              │     │  Hub/Sat/Link │     │   Vault)     │     │   Schema)    │
│  9 Tables       │     │  9 STG Tables │     │  9H / 9S / 5L│     │  3 PIT / 1 BRG│    │  7 DIM / 4 FCT│
│  58,813 Records │     │              │     │              │     │  2 BUS       │     │              │
└─────────────────┘     └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                         ◄─────────── ETL_Control (Batch/Step Logging, Watermarks, Error Handling) ──────────►
```

### Key Numbers

| Metric | Value |
|--------|-------|
| Total Database Objects | 67 across 6 databases |
| SSIS Packages | 60 (3 master + 8 orchestrator + 49 child) |
| ETL Steps | 49 per pipeline run |
| Source Tables | 9 (3 lookup + 3 reference + 3 transaction) |
| Source Records | 58,813 (1,000 taxpayers, 4 years: 2020–2023) |
| Hash Algorithm | SHA-256 via HASHBYTES('SHA2_256'), stored as VARBINARY(32) |

### Challenges Addressed

| # | Challenge | Data Vault 2.0 Solution | POC Evidence |
|---|-----------|------------------------|--------------|
| 1 | **Schema Rigidity** | Hub-Satellite separation — add new Satellites without modifying existing objects | Zero impact on all 60 SSIS packages |
| 2 | **Historical Data Loss** | Insert-only Satellites with HashDiff — every version preserved with timestamps | Complete audit trail across all entities |
| 3 | **Scalable ETL** | BatchLog + StepLog + SSIS OnError Event Handlers — step-level error isolation | Failed steps recoverable without re-running pipeline |
| 4 | **Business Rules Gap** | Silver (Business Vault) + Gold (Star Schema) — derived compliance scores, risk levels | Pre-computed metrics for BI consumption |

---

## Repository Structure

```
dv2-tax-data-warehouse-using-medallion/
│
├── DV2_TaxSystem/                              # SSIS Project (Visual Studio 2026)
│   ├── DataVaultDWH_TaxSystem.dtproj           #   Project file
│   ├── DataVaultDWH_TaxSystem.slnx             #   Solution file
│   ├── DataVaultDWH_TaxSystem.database         #   Database reference
│   ├── Project.params                          #   SSIS project parameters
│   │
│   ├── CM_TaxSystemDB.conmgr                   # 7 Connection Managers
│   ├── CM_ETL_Control.conmgr                   #   ETL Control database
│   ├── CM_ETL_Control_OnError.conmgr           #   Separate connection for OnError handlers
│   ├── CM_DV_Staging.conmgr                    #   Staging database
│   ├── CM_DV_Bronze.conmgr                     #   Bronze database
│   ├── CM_DV_Silver.conmgr                     #   Silver database
│   ├── CM_DV_Gold.conmgr                       #   Gold database
│   │
│   ├── Master_Complete_Pipeline.dtsx           # 3 Master Packages
│   ├── Master_Full_Load.dtsx                   #   Full load pipeline
│   ├── Master_Incremental_Load.dtsx            #   Incremental load pipeline
│   │
│   ├── STG_Full_Load_All.dtsx                  # Staging Layer (9 child + 2 orchestrator)
│   ├── STG_Incremental_Load_All.dtsx           #
│   ├── STG_Category.dtsx                       #
│   ├── STG_Structure.dtsx                      #
│   ├── STG_Activity.dtsx                       #
│   ├── STG_Taxpayer.dtsx                       #
│   ├── STG_Owner.dtsx                          #
│   ├── STG_Officer.dtsx                        #
│   ├── STG_MonthlyDeclaration.dtsx             #
│   ├── STG_AnnualDeclaration.dtsx              #
│   ├── STG_Payment.dtsx                        #
│   │
│   ├── BRZ_Load_All_Hubs.dtsx                  # Bronze Layer (23 child + 3 orchestrator)
│   ├── BRZ_Load_All_Satellites.dtsx            #
│   ├── BRZ_Load_All_Links.dtsx                 #
│   ├── BRZ_HUB_Category.dtsx                   #   9 Hub packages
│   ├── BRZ_HUB_Structure.dtsx                  #
│   ├── BRZ_HUB_Activity.dtsx                   #
│   ├── BRZ_HUB_Taxpayer.dtsx                   #
│   ├── BRZ_HUB_Owner.dtsx                      #
│   ├── BRZ_HUB_Officer.dtsx                    #
│   ├── BRZ_HUB_Declaration.dtsx                #
│   ├── BRZ_HUB_AnnualDeclaration.dtsx          #
│   ├── BRZ_HUB_Payment.dtsx                    #
│   ├── BRZ_SAT_Category.dtsx                   #   9 Satellite packages
│   ├── BRZ_SAT_Structure.dtsx                  #
│   ├── BRZ_SAT_Activity.dtsx                   #
│   ├── BRZ_SAT_Taxpayer.dtsx                   #
│   ├── BRZ_SAT_Owner.dtsx                      #
│   ├── BRZ_SAT_Officer.dtsx                    #
│   ├── BRZ_SAT_MonthlyDecl.dtsx                #
│   ├── BRZ_SAT_AnnualDecl.dtsx                 #
│   ├── BRZ_SAT_Payment.dtsx                    #
│   ├── BRZ_LNK_TaxpayerDeclaration.dtsx        #   5 Link packages
│   ├── BRZ_LNK_DeclarationPayment.dtsx         #
│   ├── BRZ_LNK_TaxpayerOfficer.dtsx            #
│   ├── BRZ_LNK_TaxpayerOwner.dtsx              #
│   ├── BRZ_LNK_TaxpayerAnnualDecl.dtsx         #
│   │
│   ├── SLV_Load_All.dtsx                       # Silver Layer (6 child + 1 orchestrator)
│   ├── SLV_PIT_Taxpayer.dtsx                   #   3 Point-in-Time packages
│   ├── SLV_PIT_Declaration.dtsx                #
│   ├── SLV_PIT_Payment.dtsx                    #
│   ├── SLV_BRG_Taxpayer_Owner.dtsx             #   1 Bridge package
│   ├── SLV_BUS_ComplianceScore.dtsx            #   2 Business Vault packages
│   ├── SLV_BUS_MonthlyMetrics.dtsx             #
│   │
│   ├── GLD_Load_All_Dimentions.dtsx            # Gold Layer (11 child + 2 orchestrator)
│   ├── GLD_Load_All_Facts.dtsx                 #
│   ├── GLD_DIM_Category.dtsx                   #   7 Dimension packages (5 SCD1 + 2 SCD2)
│   ├── GLD_DIM_Structure.dtsx                  #
│   ├── GLD_DIM_Activity.dtsx                   #
│   ├── GLD_DIM_PaymentMethod.dtsx              #
│   ├── GLD_DIM_Status.dtsx                     #
│   ├── GLD_DIM_Taxpayer.dtsx                   #   SCD Type 2
│   ├── GLD_DIM_Officer.dtsx                    #   SCD Type 2
│   ├── GLD_FACT_MonthlyDeclaration.dtsx        #   4 Fact packages
│   ├── GLD_FACT_Payment.dtsx                   #
│   ├── GLD_FACT_MonthlySnapshot.dtsx           #
│   └── GLD_FACT_DeclarationLifecycle.dtsx      #
│
├── Scripts/
│   ├── sql-scripts/                            # Core SQL scripts (execute in numerical order)
│   │   ├── 00_CleanAll_FreshFullLoad.sql        #   Reset all 6 databases for fresh start
│   │   ├── 01_CreateDatabaseStructure.sql       #   Source database (TaxSystemDB) DDL
│   │   ├── 02_TransactionData.sql               #   Sample data generation (1,000 taxpayers)
│   │   ├── 03_ETL_Control_Setup.sql             #   ETL Control framework (9 tables, stored procs)
│   │   └── 04_DDL_Architecture_DW.sql           #   Data warehouse DDL (49 tables across 4 layers)
│   │
│   └── verification/                           # Testing, validation, and POC scripts
│       ├── 10_Verify_FullLoad.sql               #   Full load verification queries
│       ├── 11_Verify_IncrementalLoad.sql        #   Incremental load verification queries
│       ├── 11_Verify_IncrementalLoad_Clean.sql  #   Clean output for thesis screenshots
│       ├── 12_IncrementalTest_SourceChanges.sql #   Delta test data (updates + inserts)
│       ├── 13_POC_Demonstrations.sql            #   All 4 POC demonstrations (Chapter 5)
│       └── Source_Database_Verification.sql     #   12-point source database validation
│
├── Documents/                                  # Thesis documents
│   ├── Final_Report.docx                        #   Complete thesis (6 chapters)
│   ├── Final_Presentation.pptx                  #   Defense presentation (13 slides)
│   ├── Technical_Implementation_Guide.docx      #   SSIS package build guide
│   ├── Deployment_Operations_Guide.docx         #   Google Cloud VM deployment guide
│   ├── POC_Implementation_Guide.docx            #   Step-by-step POC demonstration guide
│   ├── Source_Database_Verification.docx        #   Source DB verification (12-point checklist)
│   └── Figure_3_1.pptx                          #   Architecture diagram (editable source)
│
├── Figures/                                    # Chapter 5 POC evidence screenshots
│   ├── Figure_5.1_Schema_Flexibility.png        #   Challenge 1: Zero-impact schema evolution
│   ├── Figure_5.2_Historical_Tracking.png       #   Challenge 2: Insert-only audit trail
│   ├── Figure_5.3_ETL_Control_Framework.png     #   Challenge 3: Step-level error recovery
│   └── Figure_5.4_Business_Rules.png            #   Challenge 4: Business Vault derivation
│
├── .gitignore
├── LICENSE                                     # MIT License
└── README.md
```

---

## Quick Start

### Prerequisites

- **SQL Server 2025** Developer or Enterprise Edition
- **SQL Server Management Studio (SSMS)**
- **Visual Studio 2026** with SQL Server Integration Services (SSIS 2022+) extension
- Minimum 8 GB RAM, 20 GB free disk space

### Step 1: Create Databases and Load Source Data

Execute the SQL scripts in SSMS in numerical order:

```sql
-- 1. Create the source database schema
-- Execute: Scripts/sql-scripts/01_CreateDatabaseStructure.sql

-- 2. Generate sample tax data (1,000 taxpayers, 4 years)
-- Execute: Scripts/sql-scripts/02_TransactionData.sql

-- 3. Create ETL control framework (logging, watermarks, error handling)
-- Execute: Scripts/sql-scripts/03_ETL_Control_Setup.sql

-- 4. Create data warehouse databases (Staging, Bronze, Silver, Gold — 49 tables)
-- Execute: Scripts/sql-scripts/04_DDL_Architecture_DW.sql
```

### Step 2: Run the ETL Pipeline

Open the SSIS project in Visual Studio and execute the master package:

1. **Full Load** (first-time load of all data):
   - Open `DV2_TaxSystem/DataVaultDWH_TaxSystem.dtproj` in Visual Studio
   - Run `Master_Complete_Pipeline.dtsx` — automatically selects the Full Load path
   - All 49 steps execute across Staging → Bronze → Silver → Gold

2. **Incremental Load** (delta changes only):
   - Apply test changes: run `Scripts/verification/12_IncrementalTest_SourceChanges.sql` in SSMS
   - Run `Master_Complete_Pipeline.dtsx` — automatically selects the Incremental Load path
   - Watermark-based delta extraction processes only changed records

### Step 3: Verify Results

```sql
-- After Full Load
-- Execute: Scripts/verification/10_Verify_FullLoad.sql

-- After Incremental Load
-- Execute: Scripts/verification/11_Verify_IncrementalLoad.sql

-- Source Database Validation (12-point checklist)
-- Execute: Scripts/verification/Source_Database_Verification.sql

-- POC Demonstrations (Chapter 5 evidence)
-- Execute: Scripts/verification/13_POC_Demonstrations.sql
```

### Fresh Restart

To reset all databases and start over:

```sql
-- WARNING: Deletes ALL data across all 6 databases
-- Execute: Scripts/sql-scripts/00_CleanAll_FreshFullLoad.sql
```

> For detailed SSIS package configuration and build instructions, see [Technical Implementation Guide](Documents/Technical_Implementation_Guide.docx).

---

## Architecture Details

### Six Databases

| Database | Layer | Purpose | Objects |
|----------|-------|---------|---------|
| **TaxSystemDB** | Source | Simulated Cambodia tax system | 9 tables, 58,813 records |
| **ETL_Control** | Control | Pipeline orchestration and monitoring | 5 active + 4 framework tables |
| **DV_Staging** | Staging | Landing zone (truncate-reload / watermark delta) | 9 staging tables |
| **DV_Bronze** | Bronze | Raw Data Vault (insert-only, full history) | 9 Hubs, 9 Satellites, 5 Links |
| **DV_Silver** | Silver | Business Vault (derived rules and metrics) | 3 PIT, 1 Bridge, 2 Business tables |
| **DV_Gold** | Gold | Star Schema (BI-ready dimensional model) | 7 Dimensions, 4 Fact tables |

### ETL Control Framework

The ETL_Control database provides centralized pipeline management:

- **ETL_Process** — Registry of ETL processes with source/target mappings
- **ETL_BatchLog** — Batch-level execution tracking (start, end, status, record counts)
- **ETL_StepLog** — Step-level logging for each of 49 pipeline steps
- **ETL_ErrorLog** — Detailed error capture via SSIS OnError Event Handlers (severity, source, message)
- **ETL_Watermark** — High-water mark values for incremental loading per table

### SSIS Package Hierarchy

```
Master_Complete_Pipeline.dtsx                    ← Entry point (evaluates load type)
├── [FULL] → Master_Full_Load.dtsx
│   ├── STG_Full_Load_All.dtsx          → 9 staging child packages
│   ├── BRZ_Load_All_Hubs.dtsx          → 9 hub child packages
│   ├── BRZ_Load_All_Satellites.dtsx    → 9 satellite child packages
│   ├── BRZ_Load_All_Links.dtsx         → 5 link child packages
│   ├── SLV_Load_All.dtsx              → 6 silver child packages
│   ├── GLD_Load_All_Dimentions.dtsx   → 7 dimension child packages
│   └── GLD_Load_All_Facts.dtsx        → 4 fact child packages
│
└── [INCREMENTAL] → Master_Incremental_Load.dtsx
    └── (same orchestrator → child structure, watermark-based delta)

Total: 3 master + 8 orchestrator + 49 child = 60 SSIS packages
```

### Data Vault 2.0 Pattern

- **Hubs**: Unique business keys with SHA-256 hash keys — insert-only, never updated
- **Satellites**: Descriptive attributes with HashDiff change detection — new row per change, end-dated
- **Links**: Relationships between Hubs — insert-only composite hash keys
- **PIT Tables**: Point-in-Time snapshots for efficient temporal queries
- **Bridge Table**: Pre-joined Taxpayer-Owner relationships with current attributes
- **Business Vault**: Derived metrics (ComplianceScore 0–100, MonthlyMetrics aggregations)

---

## Documentation

| Document | Description |
|----------|-------------|
| [Final Report](Documents/Final_Report.docx) | Complete 6-chapter thesis |
| [Final Presentation](Documents/Final_Presentation.pptx) | 13-slide defense presentation with speaker notes |
| [Technical Implementation Guide](Documents/Technical_Implementation_Guide.docx) | Step-by-step SSIS package build and configuration guide |
| [Deployment Guide](Documents/Deployment_Operations_Guide.docx) | Google Cloud Platform VM deployment instructions |
| [POC Implementation Guide](Documents/POC_Implementation_Guide.docx) | Reproducible SQL scripts for 4 POC demonstrations |
| [Source Database Verification](Documents/Source_Database_Verification.docx) | 12-point source database validation with SSMS screenshots |

---

## Technology Stack

| Component | Version / Detail |
|-----------|-----------------|
| Database Engine | SQL Server 2025 Developer Edition (RTM v17.0.1000.7) |
| ETL Tool | SQL Server Integration Services (SSIS) 2022 |
| IDE | Visual Studio 2026 Community + SSIS Extension |
| Deployment | Google Cloud Platform e2-standard-4 VM (4 vCPU, 16 GB RAM) |
| OS | Windows Server 2025 Datacenter |
| Methodology | Data Vault 2.0 (Linstedt & Olschimke, 2015) |
| Architecture | Medallion (Staging → Bronze → Silver → Gold) |
| Hash Algorithm | SHA-256 via HASHBYTES('SHA2_256'), stored as VARBINARY(32) |

---

## References

This implementation is based on:

- Linstedt, D. & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann.
- Hultgren, P. (2012). *Modeling the Agile Data Warehouse with Data Vault*. New Hamilton.
- Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit*, 3rd Edition. Wiley.
- Databricks. (2025). *Medallion Lakehouse Architecture*. Databricks Documentation.
- Microsoft. (2025). *Implement Medallion Lakehouse Architecture in Microsoft Fabric*. Microsoft Documentation.

Full reference list (34 citations) available in the [Final Report](Documents/Final_Report.docx).

---

## Author

**Mr. Sot So**
- Chief of Data Management Bureau, General Department of Taxation, Cambodia
- Master of Science in Data Science and Engineering, Royal University of Phnom Penh

**Supervisor:** Mr. Chap Chanpiseth

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## Disclaimer

This is an academic proof-of-concept implementation. The tax data is **simulated** and does not contain any real taxpayer information. This project is a personal master's degree research project and is not affiliated with the General Department of Taxation's production systems.
