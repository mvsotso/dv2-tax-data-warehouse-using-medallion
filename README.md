# Data Vault 2.0 Implementation for Tax System Data Warehouse Using Medallion Architecture

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![SQL Server 2025](https://img.shields.io/badge/SQL%20Server-2025-red.svg)](https://www.microsoft.com/sql-server)
[![SSIS](https://img.shields.io/badge/SSIS-2022-orange.svg)](https://learn.microsoft.com/sql/integration-services)
[![Data Vault 2.0](https://img.shields.io/badge/Data%20Vault-2.0-green.svg)](https://datavaultalliance.com)
[![Medallion Architecture](https://img.shields.io/badge/Medallion-Architecture-teal.svg)](https://learn.microsoft.com/azure/databricks/lakehouse/medallion)

This is the implementation part of my master's thesis at Royal University of Phnom Penh. It builds a working data warehouse for Cambodia's tax administration system using Data Vault 2.0 combined with the Medallion Architecture pattern (Staging → Bronze → Silver → Gold) on SQL Server 2025 with SSIS.

The goal was to address four common challenges in traditional data warehousing — schema rigidity, lack of historical tracking, fragile ETL pipelines, and mixed raw/derived data — by applying DV2 principles in a real-world tax domain.

---

## What's in this repo

- 67 database objects across 6 databases
- 60 SSIS packages (3 Master + 8 Orchestrator + 49 Child)
- 49 automated ETL pipeline steps
- Sample data: 58,813 records covering 1,000 taxpayers from 2020–2023
- Bronze layer: 9 Hubs, 9 Satellites, 5 Links (SHA-256 hash keys)
- Gold layer: 7 Dimensions (SCD Type 1 & 2), 4 Fact tables

---

## Architecture

The pipeline moves data through four layers:

```
Source (OLTP)          Staging           Bronze (Raw Vault)      Silver (Business Vault)    Gold (Star Schema)
+--------------+    +-----------+    +--------------------+    +--------------------+    +-----------------+
| TaxSystemDB  | -> | DV_Staging| -> | DV_Bronze          | -> | DV_Silver          | -> | DV_Gold         |
| 9 tables     |    | 9 tables  |    | 9 Hubs (SHA-256)   |    | 3 PIT Tables       |    | 7 Dimensions    |
| 58,813 rows  |    | Full/Incr |    | 9 Satellites (SCD2)|    | 1 Bridge Table     |    | 4 Fact Tables   |
|              |    | Watermark |    | 5 Links            |    | 2 Business Tables  |    | BI-Ready Schema |
+--------------+    +-----------+    +--------------------+    +--------------------+    +-----------------+
```

The ETL Control database tracks everything:

```
ETL_Control Database
+-------------------+
| ETL_BatchLog      |  <- Batch-level execution tracking
| ETL_StepLog       |  <- Step-level logging (49 steps)
| ETL_ErrorLog      |  <- Error capture with OnError handlers
| ETL_Watermark     |  <- Incremental load change detection
| ETL_Configuration |  <- Runtime parameters
+-------------------+

Pipeline Flow:  Init Batch -> Staging(9) -> Hubs(9) -> SATs(9) -> Links(5) -> Silver(6) -> Gold(11) -> Close Batch
```

---

## Data Vault 2.0 Modeling

The implementation follows the DV2 standard from Linstedt & Olschimke (2015):

- Hubs store business keys with SHA-256 hash keys (`HK_*` columns)
- Satellites store descriptive attributes with HashDiff change detection and SCD Type 2 history
- Links capture relationships between Hubs using composite hash keys

Example:

```sql
-- Hub: Business key storage
HUB_Taxpayer (HK_Taxpayer, TaxID, LoadDateTime, RecordSource)

-- Satellite: Descriptive attributes with full history
SAT_Taxpayer (HK_Taxpayer, LoadDateTime, HashDiff, BusinessName, CategoryID, ...)

-- Link: Relationship between Hubs
LNK_Declaration_Taxpayer (HK_Declaration_Taxpayer, HK_MonthlyDeclaration, HK_Taxpayer, ...)
```

Incremental loading uses watermarks:

```sql
-- ETL_Watermark tracks MAX(ModifiedDate) per table
-- Only changed records are extracted each cycle
WHERE ModifiedDate > @LastWatermark AND ModifiedDate <= @CurrentWatermark
```

---

## Proof-of-Concept Results

These four POC demonstrations correspond to Chapter 5, Section 5.2 of the thesis.

**Challenge 1 — Schema Rigidity:**
Traditional star schema needs ALTER TABLE when source systems change. With DV2, I just created a new Satellite. Result: 0 out of 67 existing objects modified, 0 out of 60 SSIS packages changed.

**Challenge 2 — Historical Tracking:**
SCD Type 1 overwrites previous values with no audit trail. DV2 Satellites are insert-only with HashDiff and timestamps. All 3 test versions were preserved (revenue: 200K → 350K → 505K).

**Challenge 3 — Scalable ETL with Error Recovery:**
Monolithic pipelines have no step-level recovery. The ETL Control framework logs each step separately with BatchLog/StepLog/ErrorLog. In testing, 7 out of 8 steps recovered and 45,065 rows were processed despite 1 deliberate failure.

**Challenge 4 — Business Rules Separation:**
Raw data and derived metrics are often mixed together. The Silver Business Vault layer computes compliance scores separately from the raw vault. Result: compliance score 72.5%, risk level MEDIUM for taxpayer TAX000001.

---

## Project Structure

```
dv2-tax-data-warehouse-using-medallion/
|
+-- DV2_TaxSystem/                    # SSIS Project (Visual Studio 2026)
|   +-- DataVaultDWH_TaxSystem.dtproj
|   +-- Master_Complete_Pipeline.dtsx # Master orchestration
|   +-- STG_*.dtsx                    # Staging layer packages (9)
|   +-- BRZ_HUB_*.dtsx               # Bronze Hub packages (9)
|   +-- BRZ_SAT_*.dtsx               # Bronze Satellite packages (9)
|   +-- BRZ_LNK_*.dtsx               # Bronze Link packages (5)
|   +-- SLV_*.dtsx                    # Silver layer packages (6)
|   +-- GLD_DIM_*.dtsx                # Gold Dimension packages (7)
|   +-- GLD_FACT_*.dtsx               # Gold Fact packages (4)
|
+-- Scripts/
|   +-- sql-scripts/                  # Core setup (execute in order)
|   |   +-- 00_CleanAll_FreshFullLoad.sql
|   |   +-- 01_CreateDatabaseStructure.sql
|   |   +-- 02_TransactionData.sql
|   |   +-- 03_ETL_Control_Setup.sql
|   |   +-- 04_DDL_Architecture_DW.sql
|   |
|   +-- verification/                 # Testing and validation
|       +-- 10_Verify_FullLoad.sql
|       +-- 11_Verify_IncrementalLoad.sql
|       +-- 12_IncrementalTest_SourceChanges.sql
|       +-- 13_POC_Demonstrations.sql
|       +-- Source_Database_Verification.sql
|
+-- Documents/                        # PDF thesis documents
|   +-- Final_Report.pdf
|   +-- Final_Presentation.pdf
|   +-- Technical_Implementation_Guide.pdf
|   +-- POC_Implementation_Guide.pdf
|   +-- Source_Database_Verification.pdf
|   +-- Deployment_Operations_Guide.pdf
|
+-- Figures/                          # POC evidence screenshots
    +-- Figure_5.1_Schema_Flexibility.png
    +-- Figure_5.2_Historical_Tracking.png
    +-- Figure_5.3_ETL_Control_Framework.png
    +-- Figure_5.4_Business_Rules.png
```

---

## How to Set Up

### Prerequisites

- SQL Server 2025 Developer Edition (or 2019+)
- Visual Studio 2022/2026 with SSIS extension
- SQL Server Management Studio (SSMS)

### Steps

1. Create the source database and tables:
   ```
   sqlcmd -i Scripts/sql-scripts/01_CreateDatabaseStructure.sql
   ```

2. Generate sample tax data (58,813 records):
   ```
   sqlcmd -i Scripts/sql-scripts/02_TransactionData.sql
   ```

3. Set up the ETL Control framework:
   ```
   sqlcmd -i Scripts/sql-scripts/03_ETL_Control_Setup.sql
   ```

4. Create the Data Warehouse schema (Bronze/Silver/Gold):
   ```
   sqlcmd -i Scripts/sql-scripts/04_DDL_Architecture_DW.sql
   ```

5. Open `DV2_TaxSystem/DataVaultDWH_TaxSystem.dtproj` in Visual Studio, then execute `Master_Complete_Pipeline.dtsx`.

### Verification

After running the pipeline, check the results:

```
sqlcmd -i Scripts/verification/10_Verify_FullLoad.sql
sqlcmd -i Scripts/verification/13_POC_Demonstrations.sql
```

---

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Database Engine | Microsoft SQL Server | 2025 Developer Edition |
| ETL Tool | SQL Server Integration Services (SSIS) | Included with SQL Server |
| Development IDE | Visual Studio Community | 2026 |
| Scripting | T-SQL | - |
| Job Scheduling | SQL Server Agent | Included |
| Cloud Platform | Google Cloud Compute Engine | e2-standard-4 |
| OS | Windows Server 2025 Datacenter | - |

All tools used are free or included with SQL Server — no extra licenses needed.

---

## Performance

| Operation | Time | Notes |
|-----------|------|-------|
| Full Load (49 steps) | 91.6 sec | All 58,813 records across 4 layers |
| Incremental Load | 97.7 sec | Delta extraction via watermark |
| Gold Query (Revenue Trends) | 0.12 sec | vs 0.29 sec on Raw Vault (2.4x faster) |
| Gold Query (Compliance) | 0.08 sec | vs 0.91 sec on Raw Vault (11.4x faster) |

---

## Documents

| Document | Description |
|----------|-------------|
| [Final Report](Documents/Final_Report.pdf) | Complete thesis — 6 chapters, 55 pages |
| [Final Presentation](Documents/Final_Presentation.pdf) | Defense slides, 20 slides |
| [Technical Implementation Guide](Documents/Technical_Implementation_Guide.pdf) | SSIS package inventory and deployment details |
| [POC Implementation Guide](Documents/POC_Implementation_Guide.pdf) | SQL scripts to reproduce all POC demos |
| [Source Database Verification](Documents/Source_Database_Verification.pdf) | 12-point data quality checklist |
| [Deployment Operations Guide](Documents/Deployment_Operations_Guide.pdf) | Production deployment procedures |

---

## Citation

If you use this work in your research:

```bibtex
@mastersthesis{so2026dv2,
  title     = {Data Vault 2.0 Implementation for Cambodia Tax System Data Warehouse Using Medallion Architecture},
  author    = {So, Sot},
  year      = {2026},
  school    = {Royal University of Phnom Penh},
  type      = {Master's Thesis},
  program   = {Master of Science in Data Science and Engineering},
  note      = {Supervisor: Mr. Chap Chanpiseth}
}
```

## References

- Linstedt, D. & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann.
- [Databricks Medallion Architecture](https://docs.databricks.com/en/lakehouse/medallion.html)
- [Scalefree Data Vault Alliance](https://www.scalefree.com/consulting/data-vault-2-0/)
- [Microsoft SSIS Documentation](https://learn.microsoft.com/sql/integration-services)

---

## License

MIT License — see [LICENSE](LICENSE).

## Author

Sot So — MSc Data Science and Engineering, Royal University of Phnom Penh, Faculty of Engineering

Supervisor: Mr. Chap Chanpiseth | March 2026
