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
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   TaxSystemDB   │────▶│  DV_Staging   │────▶│  DV_Bronze   │────▶│  DV_Silver   │────▶│  DV_Gold     │
│   (Source)      │     │  (Landing)    │     │  (Raw Vault) │     │  (Business   │     │  (Star       │
│                 │     │              │     │  Hub/Sat/Link │     │   Vault)     │     │   Schema)    │
│  9 Tables       │     │  9 STG Tables │     │  9H/9S/5L    │     │  3 PIT/1 BRG │     │  7 DIM/4 FCT │
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
| Total Database Objects | 67 across 6 databases |
| SSIS Packages | 60 total (3 master + 8 orchestrator + 49 child) |

---

## 📁 Repository Structure

```
dv2-tax-data-warehouse-using-medallion/
│
├── DV2_TaxSystem/                              # SSIS Project (60 packages + project files)
│   ├── DataVaultDWH_TaxSystem.dtproj            # Visual Studio SSIS project file
│   ├── DataVaultDWH_TaxSystem.slnx              # Solution file
│   ├── DataVaultDWH_TaxSystem.database          # Database reference
│   ├── Project.params                           # SSIS project parameters
│   │
│   ├── CM_TaxSystemDB.conmgr                    # 7 connection managers
│   ├── CM_ETL_Control.conmgr                    #   ETL Control database
│   ├── CM_ETL_Control_OnError.conmgr            #   Separate connection for OnError handlers
│   ├── CM_DV_Staging.conmgr                     #   Staging database
│   ├── CM_DV_Bronze.conmgr                      #   Bronze database
│   ├── CM_DV_Silver.conmgr                      #   Silver database
│   ├── CM_DV_Gold.conmgr                        #   Gold database
│   │
│   ├── Master_Complete_Pipeline.dtsx            # 3 master packages
│   ├── Master_Full_Load.dtsx                    #   Full load path
│   ├── Master_Incremental_Load.dtsx             #   Incremental load path
│   │
│   ├── STG_Full_Load_All.dtsx                   # 11 staging packages (9 child + 2 orchestrator)
│   ├── STG_Incremental_Load_All.dtsx            #   Orchestrators: full & incremental
│   ├── STG_Category.dtsx                        #   Child: 3 lookup tables
│   ├── STG_Structure.dtsx                       #
│   ├── STG_Activity.dtsx                        #
│   ├── STG_Taxpayer.dtsx                        #   Child: 6 core tables
│   ├── STG_Owner.dtsx                           #
│   ├── STG_Officer.dtsx                         #
│   ├── STG_MonthlyDeclaration.dtsx              #
│   ├── STG_AnnualDeclaration.dtsx               #
│   ├── STG_Payment.dtsx                         #
│   │
│   ├── BRZ_Load_All_Hubs.dtsx                   # 26 bronze packages (23 child + 3 orchestrator)
│   ├── BRZ_Load_All_Satellites.dtsx             #   Orchestrators: hubs, sats, links
│   ├── BRZ_Load_All_Links.dtsx                  #
│   ├── BRZ_HUB_Category.dtsx                    #   Child: 9 hub packages
│   ├── BRZ_HUB_Structure.dtsx                   #
│   ├── BRZ_HUB_Activity.dtsx                    #
│   ├── BRZ_HUB_Taxpayer.dtsx                    #
│   ├── BRZ_HUB_Owner.dtsx                       #
│   ├── BRZ_HUB_Officer.dtsx                     #
│   ├── BRZ_HUB_Declaration.dtsx                 #
│   ├── BRZ_HUB_AnnualDeclaration.dtsx           #
│   ├── BRZ_HUB_Payment.dtsx                     #
│   ├── BRZ_SAT_Category.dtsx                    #   Child: 9 satellite packages
│   ├── BRZ_SAT_Structure.dtsx                   #
│   ├── BRZ_SAT_Activity.dtsx                    #
│   ├── BRZ_SAT_Taxpayer.dtsx                    #
│   ├── BRZ_SAT_Owner.dtsx                       #
│   ├── BRZ_SAT_Officer.dtsx                     #
│   ├── BRZ_SAT_MonthlyDecl.dtsx                 #
│   ├── BRZ_SAT_AnnualDecl.dtsx                  #
│   ├── BRZ_SAT_Payment.dtsx                     #
│   ├── BRZ_LNK_TaxpayerDeclaration.dtsx         #   Child: 5 link packages
│   ├── BRZ_LNK_DeclarationPayment.dtsx          #
│   ├── BRZ_LNK_TaxpayerOfficer.dtsx             #
│   ├── BRZ_LNK_TaxpayerOwner.dtsx               #
│   ├── BRZ_LNK_TaxpayerAnnualDecl.dtsx          #
│   │
│   ├── SLV_Load_All.dtsx                        # 7 silver packages (6 child + 1 orchestrator)
│   ├── SLV_PIT_Taxpayer.dtsx                    #   Child: 3 PIT tables
│   ├── SLV_PIT_Declaration.dtsx                 #
│   ├── SLV_PIT_Payment.dtsx                     #
│   ├── SLV_BRG_Taxpayer_Owner.dtsx              #   Child: 1 bridge table
│   ├── SLV_BUS_ComplianceScore.dtsx             #   Child: 2 business vault tables
│   ├── SLV_BUS_MonthlyMetrics.dtsx              #
│   │
│   ├── GLD_Load_All_Dimentions.dtsx             # 13 gold packages (11 child + 2 orchestrator)
│   ├── GLD_Load_All_Facts.dtsx                  #   Orchestrators: dimensions & facts
│   ├── GLD_DIM_Category.dtsx                    #   Child: 7 dimension packages (5 SCD1 + 2 SCD2)
│   ├── GLD_DIM_Structure.dtsx                   #
│   ├── GLD_DIM_Activity.dtsx                    #
│   ├── GLD_DIM_PaymentMethod.dtsx               #
│   ├── GLD_DIM_Status.dtsx                      #
│   ├── GLD_DIM_Taxpayer.dtsx                    #   SCD Type 2
│   ├── GLD_DIM_Officer.dtsx                     #   SCD Type 2
│   ├── GLD_FACT_MonthlyDeclaration.dtsx         #   Child: 4 fact packages
│   ├── GLD_FACT_Payment.dtsx                    #
│   ├── GLD_FACT_MonthlySnapshot.dtsx            #
│   └── GLD_FACT_DeclarationLifecycle.dtsx       #
│
├── Scripts/
│   ├── sql-scripts/                             # Core SQL implementation (execute in order)
│   │   ├── 00_CleanAll_FreshFullLoad.sql         # Reset all 6 databases for fresh start
│   │   ├── 01_CreateDatabaseStructure.sql        # Source database (TaxSystemDB) DDL
│   │   ├── 02_TransactionData.sql                # Simulated tax data (1,000 taxpayers)
│   │   ├── 03_ETL_Control_Setup.sql              # ETL Control framework (logging, watermarks)
│   │   └── 04_DDL_Architecture_DW.sql            # Data warehouse DDL (Staging/Bronze/Silver/Gold)
│   │
│   └── verification/                            # Testing and validation scripts
│       ├── 10_Verify_FullLoad.sql                # Full load verification (7 checks)
│       ├── 11_Verify_IncrementalLoad.sql         # Incremental load verification (8 checks)
│       ├── 11_Verify_IncrementalLoad_Clean.sql   # Clean incremental verification for screenshots
│       ├── 12_IncrementalTest_SourceChanges.sql  # Delta test data for incremental loads
│       ├── Screenshot_Source_DB_AllGrids.sql     # Source DB screenshot helper queries
│       └── Source_Database_Verification.sql      # Source database validation (12-point checklist)
│
├── Proof Of Concepts/                           # POC demonstrations (Chapter 5)
│   ├── 13_POC_Demonstrations.sql                 # All 4 POC challenge demonstrations
│   └── POC_Implementation_Guide.docx             # Step-by-step POC guide
│
├── Documents/                                   # Thesis documents
│   ├── Final_Report.docx                         # Complete thesis report (6 chapters, ~80 pages)
│   ├── Final_Presentation.pptx                   # Defense presentation (13 slides)
│   ├── Technical_Implementation_Guide.docx       # SSIS package build guide (~150 pages)
│   ├── Deployment_Operations_Guide.docx          # Google Cloud VM deployment & benchmarks
│   ├── Source_Database_Verification.docx         # Source DB verification results (12-point checklist)
│   └── Figure_3_1.pptx                           # Architecture diagram (editable)
│
├── Figures/                                     # Chapter 5 POC screenshots
│   ├── Figure_5.1_Schema_Flexibility.png         # Challenge 1: Schema flexibility demo
│   ├── Figure_5.2_Historical_Tracking.png        # Challenge 2: Historical tracking demo
│   ├── Figure_5.3_ETL_Control_Framework.png      # Challenge 3: ETL control framework demo
│   ├── Figure_5.4_Business_Rules.png             # Challenge 4: Business rules separation demo
│   └── Screenshot_Capture_Guide.docx             # Guide for capturing SSMS screenshots
│
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- **SQL Server 2025** (Developer or Enterprise Edition)
- **SQL Server Management Studio (SSMS)** 2022
- **Visual Studio 2026** with SQL Server Integration Services (SSIS 2022+) extension
- Minimum 8 GB RAM, 20 GB free disk space

### Installation

Execute the SQL scripts in numerical order against your SQL Server instance:

```bash
# Step 1: Create source database and load test data
sqlcmd -S localhost -i Scripts/sql-scripts/01_CreateDatabaseStructure.sql
sqlcmd -S localhost -i Scripts/sql-scripts/02_TransactionData.sql

# Step 2: Create ETL control framework
sqlcmd -S localhost -i Scripts/sql-scripts/03_ETL_Control_Setup.sql

# Step 3: Create data warehouse databases (Staging, Bronze, Silver, Gold)
sqlcmd -S localhost -i Scripts/sql-scripts/04_DDL_Architecture_DW.sql

# Step 4: Open DV2_TaxSystem/ project in Visual Studio and run SSIS packages
# Note: ETL stored procedures (staging, bronze, silver, gold, orchestration)
# are embedded within the SSIS packages and execute via Execute SQL Tasks.
```

### Running the ETL Pipeline

The ETL pipeline is executed through **SSIS packages** in Visual Studio:

1. **Full Load** (first-time load of all data)
   - Open `DV2_TaxSystem/DataVaultDWH_TaxSystem.dtproj` in Visual Studio
   - Run `Master_Complete_Pipeline.dtsx` → selects the Full Load path
   - All 49 steps execute across Staging → Bronze → Silver → Gold

2. **Incremental Load** (delta changes only)
   - Apply test changes first: run `Scripts/verification/12_IncrementalTest_SourceChanges.sql` in SSMS
   - Run `Master_Complete_Pipeline.dtsx` → selects the Incremental Load path
   - Watermark-based delta extraction processes only changed records

> See `Documents/Technical_Implementation_Guide.docx` for complete step-by-step package build instructions.

### Verifying Results

```sql
-- After Full Load
-- Run: Scripts/verification/10_Verify_FullLoad.sql

-- After Incremental Load
-- Run: Scripts/verification/11_Verify_IncrementalLoad.sql

-- Source Database Validation (12-point checklist)
-- Run: Scripts/verification/Source_Database_Verification.sql

-- POC Demonstrations (Chapter 5)
-- Run: Proof Of Concepts/13_POC_Demonstrations.sql
```

---

## 🏗️ Architecture Details

### Six Databases

| Database | Purpose | Key Objects |
|----------|---------|-------------|
| **TaxSystemDB** | Source system (simulated) | 9 tables (3 lookup + 3 reference + 3 transaction) |
| **ETL_Control** | Pipeline orchestration | ETL_Process, BatchLog, StepLog, ErrorLog, Watermark |
| **DV_Staging** | Landing zone | 9 staging tables (truncate & reload / watermark delta) |
| **DV_Bronze** | Raw Data Vault | 9 Hubs, 9 Satellites, 5 Links |
| **DV_Silver** | Business Vault | 3 PIT tables, 1 Bridge, 2 Business Vault tables |
| **DV_Gold** | Star Schema | 7 Dimensions (5 SCD1 + 2 SCD2), 4 Fact tables |

### Data Vault 2.0 Pattern

- **Hubs**: Business keys with SHA2_256 hash keys (HUB_Taxpayer, HUB_Declaration, etc.)
- **Satellites**: Descriptive attributes with HashDiff change detection and end-dating
- **Links**: Many-to-many relationships (LNK_TaxpayerDeclaration, LNK_DeclarationPayment, etc.)
- **PIT Tables**: Point-in-Time snapshots for efficient temporal queries
- **Bridge Table**: Pre-joined Taxpayer-Owner relationships

### ETL Control Framework

- **Process registry**: ETL_Process table maps each pipeline step to its source and target
- **Batch-level logging**: Every ETL run tracked with BatchID, status, record counts
- **Step-level logging**: Each of 49 steps individually tracked
- **Watermark-based incremental**: Per-table watermarks for delta extraction
- **Error handling**: Centralized error logging with severity classification (SSIS OnError Event Handler)

### SSIS Package Hierarchy (60 Packages)

```
Master_Complete_Pipeline.dtsx
├── [FULL] → Master_Full_Load.dtsx
│   ├── STG_Full_Load_All.dtsx → 9 staging child packages
│   ├── BRZ_Load_All_Hubs.dtsx → 9 hub child packages
│   ├── BRZ_Load_All_Satellites.dtsx → 9 satellite child packages
│   ├── BRZ_Load_All_Links.dtsx → 5 link child packages
│   ├── SLV_Load_All.dtsx → 6 silver child packages
│   ├── GLD_Load_All_Dimentions.dtsx → 7 dimension child packages
│   └── GLD_Load_All_Facts.dtsx → 4 fact child packages
│
└── [INCREMENTAL] → Master_Incremental_Load.dtsx
    └── (same structure, watermark-based delta extraction)

Total: 3 master + 8 orchestrator + 49 child = 60 SSIS packages
```

---

## 📊 Performance Benchmarks

### ETL Load Performance

| Metric | Full Load | Incremental Load |
|--------|-----------|------------------|
| **Total Records** | 336,677 | 206,780 |
| **Total Duration** | 120.9s | 93.6s |
| **Throughput** | 2,785 rec/s | 2,209 rec/s |
| Staging Layer | 22.8s (58,813 records) | 6.0s (58,824 records) |
| Bronze Layer | 39.5s (184,108 records) | 30.3s (50 records) |
| Silver Layer | 3.4s (2,004 records) | 3.5s (57,186 records) |
| Gold Layer | 11.0s (91,752 records) | 12.7s (90,729 records) |

### Query Performance (Gold Star Schema vs Raw Vault)

| Query Complexity | Query Description | Gold (ms) | Raw Vault (ms) | Speedup |
|-----------------|-------------------|-----------|----------------|---------|
| Simple | Total tax by category (2-table join) | 24 | 199 | **8.3×** |
| Medium | Top 10 taxpayers by payment (4-table join) | 18 | 123 | **6.8×** |
| Complex | Monthly revenue trend with category and status (4-table join) | 771 | 798 | **1.03×** |

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [Final Report](Documents/Final_Report.docx) | Complete 6-chapter thesis (~80 pages) |
| [Final Presentation](Documents/Final_Presentation.pptx) | 13-slide defense presentation with speaker notes |
| [Technical Implementation Guide](Documents/Technical_Implementation_Guide.docx) | Step-by-step SSIS package build guide (~150 pages) |
| [Deployment Guide](Documents/Deployment_Operations_Guide.docx) | Google Cloud Platform VM deployment & benchmarks |
| [Source Database Verification](Documents/Source_Database_Verification.docx) | 12-point source DB validation with screenshots |
| [POC Implementation Guide](Proof%20Of%20Concepts/POC_Implementation_Guide.docx) | Step-by-step guide for 4 POC demonstrations |

---

## 🔧 Technology Stack

- **Database**: SQL Server 2025 Developer Edition (v17, RTM 17.0.1000.7)
- **ETL**: SQL Server Integration Services (SSIS) 2022
- **IDE**: Visual Studio 2026 + SSIS 2022+ Extension
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

Full reference list (34 citations) available in the [Final Report](Documents/Final_Report.docx).

---

## 👤 Author

**Mr. Sot So**
- Chief of Data Management Bureau, General Department of Taxation, Cambodia
- Master of Science in Data Science and Engineering
- Royal University of Phnom Penh

**Supervisor**: Mr. Chap Chanpiseth

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## ⚠️ Disclaimer

This is an academic proof-of-concept implementation. The tax data is **simulated** and does not contain any real taxpayer information. This project is a personal master's degree research project and is not affiliated with the General Department of Taxation's production systems.
