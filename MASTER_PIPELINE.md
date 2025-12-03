# Master Pipeline Documentation

## Overview
The `master_pipeline.py` script orchestrates the monthly data processing workflow for KBB MF. It automates the execution of a series of Python scripts that process invoices (NF/NFI), update entry tables, calculate inventory, and generate final reports.

The pipeline is designed to be **idempotent** and **efficient**: it checks file modification timestamps to determine if a step needs to be re-run. If inputs haven't changed since the last successful run, the step is skipped.

## Usage
Run the pipeline from the terminal:
```bash
python master_pipeline.py --year YYYY --month MM [options]
```

### Arguments
- `--year`, `-y`: Target Year (e.g., 2024)
- `--month`, `-m`: Target Month (e.g., 10)
- `--step`, `-s`: Run only a specific step (e.g., `step1_nfi`)
- `--start-from`: Resume pipeline from a specific step
- `--force`, `-f`: Force execution of all steps, ignoring dependency checks

## Pipeline Steps
The pipeline executes the following scripts in order:

| Step Key | Script Name | Description |
| :--- | :--- | :--- |
| `step1_nfi` | `NFI_1_Create.py` | Processes NFI (Nota Fiscal de Importação) XMLs. |
| `step1_nf` | `NF_1_Create.py` | Processes NF (Nota Fiscal) XMLs. (Optional) |
| `step2_nf_agg` | `NF_2_Aggregate.py` | Aggregates individual NF files. |
| `step2_nfi_agg` | `NFI_2_Aggregate.py` | Aggregates individual NFI files. |
| `step2_5_process_data` | `process_data.py` | Processes RAW files into CLEAN files. |
| `step3_update_entradas` | `Atualiza_Entradas.py` | Updates the main `T_Entradas.xlsx` table with new invoice data. |
| **MANUAL STEP** | N/A | **Pause**: User must open, save, and close `T_Entradas.xlsx` to recalculate formulas. |
| `step4_inventory` | `process_inv.py` | Processes inventory data (`R_Estoq`). |
| `step5_report` | `remake_dataset01.py` | Generates the final `Kon_Report`. |

## Dependency Checks & Logic
The pipeline uses a `check_dependencies` function to decide whether to run a step. It compares the modification time (`mtime`) of input files against output files.

### 1. Invoice Creation (`step1_nfi`, `step1_nf`)
- **Inputs**: XML files in `.../nfs/{year}/Serie X/{month_name}`.
- **Outputs**: Excel files in `.../Mauricio/Contabilidade - Tsuriel`.
- **Logic**: Re-runs if **any** XML file in the source folder is newer than the latest output file for that month.

### 2. Aggregation (`step2_nf_agg`, `step2_nfi_agg`)
- **Inputs**: 
    - Level 1 Output files (from Step 1).
- **Outputs**: `..._todos.xlsx` (Aggregated file).
- **Logic**: Re-runs if new Level 1 files exist.

### 2.5. Process Data (`step2_5_process_data`)
- **Inputs**: RAW files in `.../Fechamentos/data/raw`.
- **Outputs**: CLEAN files in `.../Fechamentos/data/clean`.
- **Logic**: Always runs. The script itself handles file checking (skips if clean file is newer).

### 3. Update Entradas (`step3_update_entradas`)
- **Inputs**: Aggregated files (`NF_..._todos.xlsx`, `NFI_..._todos.xlsx`).
- **Outputs**: `T_Entradas.xlsx`.
- **Logic**: Re-runs if the aggregated invoice files are newer than `T_Entradas.xlsx`.
- **Note**: This step appends/updates data in `T_Entradas.xlsx`.

### 4. Inventory (`step4_inventory`)
- **Inputs**: 
    - `T_Entradas.xlsx`
    - Raw Inventory files in `.../clean/{year}_{month}`
- **Outputs**: `R_Estoq_fdm_{year}_{month}.xlsx`
- **Logic**: Re-runs if `T_Entradas` or any inventory file is newer than the output `R_Estoq` file.

### 5. Report (`step5_report`)
- **Inputs**: 
    - `R_Estoq_fdm_{year}_{month}.xlsx`
    - `T_Entradas.xlsx`
- **Outputs**: `Kon_Report_{year}_{month}.xlsx`
- **Logic**: Re-runs if the inventory report or entries table are newer than the final report.

## Data Storage & Paths
The pipeline operates on files stored in **Dropbox**. It attempts to automatically locate the base directory from the following candidates:
1. `/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/`
2. `/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data`

### Key Directories
- **NFS**: `.../Dropbox/nfs` (Source XMLs and Level 1 Outputs)
- **Tables**: `.../Fechamentos/data/Tables` (Reference tables like `T_NFTipo`, `T_Entradas`)
- **Clean Data**: `.../Fechamentos/data/clean/{year}_{month}` (Inventory files and Final Reports)
