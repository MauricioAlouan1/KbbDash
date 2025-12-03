import os
import sys
import argparse
import importlib.util
from datetime import datetime
import pandas as pd

# Define paths and script names
SCRIPTS = {
    "step1_nfi": "NFI_1_Create",
    "step1_nf": "NF_1_Create", # Optional
    "step2_nf_agg": "NF_2_Aggregate",
    "step2_nfi_agg": "NFI_2_Aggregate",
    "step3_update_entradas": "Atualiza_Entradas",
    "step4_inventory": "process_inv",
    "step5_report": "remake_dataset01"
}

def load_module(script_name):
    """Dynamically load a module from the current directory."""
    try:
        spec = importlib.util.spec_from_file_location(script_name, f"{script_name}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[script_name] = module
        spec.loader.exec_module(module)
        return module
    except FileNotFoundError:
        print(f"⚠️ Script {script_name}.py not found.")
        return None
    except Exception as e:
        print(f"❌ Error loading {script_name}: {e}")
        return None

def run_step(step_name, module, year, month):
    print(f"\n🚀 Running {step_name} ({module.__name__})...")
    try:
        # Check if module has a main function that accepts args
        if hasattr(module, 'main'):
            # Inspect signature or just try calling with args
            try:
                module.main(year, month)
            except TypeError:
                # Fallback for scripts that might not accept args yet or have different signature
                print(f"⚠️ {step_name} main() might not accept args. Trying without...")
                module.main()
        else:
            print(f"❌ {step_name} has no main() function.")
            return False
        print(f"✅ {step_name} completed.")
        return True
    except Exception as e:
        print(f"❌ {step_name} failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_latest_mtime_in_folder(folder_path, pattern=None):
    """Get the latest modification time of files in a folder matching a pattern."""
    if not os.path.exists(folder_path):
        return 0
    
    latest_mtime = 0
    for root, _, files in os.walk(folder_path):
        for f in files:
            if pattern and pattern not in f:
                continue
            # Ignore hidden files
            if f.startswith('.'):
                continue
            full_path = os.path.join(root, f)
            try:
                mtime = os.path.getmtime(full_path)
                if mtime > latest_mtime:
                    latest_mtime = mtime
            except OSError:
                pass
    return latest_mtime

def get_file_mtime(file_path):
    if os.path.exists(file_path):
        return os.path.getmtime(file_path)
    return 0

def prompt_manual_step(message):
    print(f"\n🛑 MANUAL STEP REQUIRED: {message}")
    print("Press ENTER once you have completed this step to continue...")
    input()
    print("Resuming pipeline...")

def check_dependencies(step_name, year, month, base_dir, force=False):
    """
    Check if a step needs to run based on input/output timestamps.
    Returns True if step should run, False otherwise.
    """
    if force:
        return True

    # Define paths
    # Assuming base_dir is .../Fechamentos/data
    # Dropbox root is needed for NFS
    dropbox_root = None
    if "/Dropbox/" in base_dir:
        dropbox_root = base_dir.split("/Dropbox/")[0] + "/Dropbox"
    else:
        # Fallback
        dropbox_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(base_dir)))))

    nfs_dir = os.path.join(dropbox_root, "nfs")
    clean_dir = os.path.join(base_dir, "clean", f"{year}_{month:02d}")
    tables_dir = os.path.join(base_dir, "Tables")
    
    # Define Inputs/Outputs for each step
    # Timestamps: 0 means missing (must run)
    
    if step_name in ["step1_nfi", "step1_nf"]:
        # Input: XML files in nfs/{year}/Serie X/{month_name}
        # Output: NFI_{year}_{month}_SerieX.xlsx
        # Logic: If ANY XML is newer than the OLDEST output file (or if output missing), run.
        # Simplification: Just check if we need to run. Since there are multiple series, 
        # it's safer to run if any XML changed.
        # We can check the latest XML mtime vs the latest NFI log file or similar.
        # Or just return True for Level 1 if we can't easily map 1:1.
        # User said: "re-run if change in invoice files".
        
        # Let's check global latest XML vs global latest Output
        # This is a heuristic.
        latest_xml = get_latest_mtime_in_folder(os.path.join(nfs_dir, str(year)), ".xml")
        
        # Check outputs in .../Mauricio/Contabilidade - Tsuriel
        output_dir = os.path.join(nfs_dir, "Mauricio", "Contabilidade - Tsuriel")
        prefix = "NFI" if "nfi" in step_name else "NF"
        # We need to check specific month files
        # Pattern: {prefix}_{year}_{month}_...
        latest_output = get_latest_mtime_in_folder(output_dir, f"{prefix}_{year}_{month}")
        
        if latest_xml > latest_output:
            print(f"🔄 {step_name}: New XMLs detected. Re-running.")
            return True
        if latest_output == 0:
            print(f"🆕 {step_name}: Output missing. Running.")
            return True
            
        print(f"⏭️ {step_name}: Up to date. Skipping.")
        return False

    elif step_name in ["step2_nf_agg", "step2_nfi_agg"]:
        # Input: Level 1 outputs + T_NFTipo
        # Output: ..._todos.xlsx
        
        # Level 1 outputs
        nfs_output_dir = os.path.join(nfs_dir, "Mauricio", "Contabilidade - Tsuriel")
        prefix = "NFI" if "nfi" in step_name else "NF"
        latest_input_l1 = get_latest_mtime_in_folder(nfs_output_dir, f"{prefix}_{year}_{month}")
        
        # T_NFTipo
        t_nftipo_mtime = get_file_mtime(os.path.join(tables_dir, "T_NFTipo.xlsx"))
        
        max_input = max(latest_input_l1, t_nftipo_mtime)
        
        # Output
        output_file = os.path.join(nfs_output_dir, f"{prefix}_{year}_{month:02d}_todos.xlsx")
        output_mtime = get_file_mtime(output_file)
        
        if max_input > output_mtime:
            print(f"🔄 {step_name}: Inputs changed. Re-running.")
            return True
        if output_mtime == 0:
            print(f"🆕 {step_name}: Output missing. Running.")
            return True
            
        print(f"⏭️ {step_name}: Up to date. Skipping.")
        return False

    elif step_name == "step3_update_entradas":
        # Input: Level 2 outputs (todos.xlsx)
        # Output: T_Entradas.xlsx (modified)
        
        nfs_output_dir = os.path.join(nfs_dir, "Mauricio", "Contabilidade - Tsuriel")
        # Check both NF and NFI todos
        nf_todos = get_file_mtime(os.path.join(nfs_output_dir, f"NF_{year}_{month:02d}_todos.xlsx"))
        nfi_todos = get_file_mtime(os.path.join(nfs_output_dir, f"NFI_{year}_{month:02d}_todos.xlsx"))
        
        max_input = max(nf_todos, nfi_todos)
        
        # Output: T_Entradas.xlsx
        # Problem: T_Entradas is also an input (accumulates).
        # We should check if the inputs are newer than the *last modification* of T_Entradas.
        # If inputs are newer, it means we have new data to append/update.
        t_entradas_path = os.path.join(tables_dir, "T_Entradas.xlsx")
        t_entradas_mtime = get_file_mtime(t_entradas_path)
        
        if max_input > t_entradas_mtime:
            print(f"🔄 {step_name}: New invoice data available. Re-running.")
            return True
            
        # Also check if Conc_Estoq changed? (Previous month)
        # For now, invoice data is the main driver.
        
        print(f"⏭️ {step_name}: T_Entradas seems up to date relative to invoices. Skipping.")
        return False

    elif step_name == "step4_inventory":
        # Input: T_Entradas (saved), Inventory Files
        # Output: R_Estoq...
        
        t_entradas_path = os.path.join(tables_dir, "T_Entradas.xlsx")
        t_entradas_mtime = get_file_mtime(t_entradas_path)
        
        # Inventory files
        latest_inv = get_latest_mtime_in_folder(clean_dir) # B_Estoq, etc.
        
        max_input = max(t_entradas_mtime, latest_inv)
        
        output_file = os.path.join(clean_dir, f"R_Estoq_fdm_{year}_{month:02d}.xlsx")
        output_mtime = get_file_mtime(output_file)
        
        if max_input > output_mtime:
            print(f"🔄 {step_name}: Inputs (Entradas/Inventory) changed. Re-running.")
            return True
        if output_mtime == 0:
            print(f"🆕 {step_name}: Output missing. Running.")
            return True
            
        print(f"⏭️ {step_name}: Up to date. Skipping.")
        return False

    elif step_name == "step5_report":
        # Input: Everything
        # Check if R_Estoq or T_Entradas changed
        
        r_estoq_path = os.path.join(clean_dir, f"R_Estoq_fdm_{year}_{month:02d}.xlsx")
        r_estoq_mtime = get_file_mtime(r_estoq_path)
        
        t_entradas_mtime = get_file_mtime(os.path.join(tables_dir, "T_Entradas.xlsx"))
        
        max_input = max(r_estoq_mtime, t_entradas_mtime)
        
        output_file = os.path.join(clean_dir, f"Kon_Report_{year}_{month:02d}.xlsx")
        output_mtime = get_file_mtime(output_file)
        
        if max_input > output_mtime:
            print(f"🔄 {step_name}: Inputs changed. Re-running.")
            return True
        if output_mtime == 0:
            print(f"🆕 {step_name}: Output missing. Running.")
            return True
            
        print(f"⏭️ {step_name}: Up to date. Skipping.")
        return False

    return True

def main():
    parser = argparse.ArgumentParser(description="Master Pipeline for KBB MF Data Processing")
    parser.add_argument("--year", "-y", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", "-m", type=int, required=True, help="Month (MM)")
    parser.add_argument("--step", "-s", type=str, help="Run specific step only")
    parser.add_argument("--start-from", type=str, help="Start from specific step")
    parser.add_argument("--force", "-f", action="store_true", help="Force run all steps (ignore dependencies)")
    
    args = parser.parse_args()
    year, month = args.year, args.month
    
    # Resolve Base Dir (needed for dependency checks)
    path_options = [
        '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
        '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
    ]
    base_dir = None
    for path in path_options:
        if os.path.exists(path):
            base_dir = path
            break
    if not base_dir:
        print("⚠️ Warning: Base directory not found. Dependency checks might fail.")
        base_dir = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/" # Default

    print(f"Starting Pipeline for {year}-{month:02d}")
    
    # Define execution order
    execution_order = [
        "step1_nfi",
        "step1_nf",
        "step2_nf_agg",
        "step2_nfi_agg",
        "step3_update_entradas",
        "step4_inventory",
        "step5_report"
    ]
    
    start_index = 0
    if args.start_from:
        if args.start_from in execution_order:
            start_index = execution_order.index(args.start_from)
        else:
            print(f"❌ Start step '{args.start_from}' not found.")
            return

    for i in range(start_index, len(execution_order)):
        step_key = execution_order[i]
        script_name = SCRIPTS[step_key]
        
        if args.step and args.step != step_key:
            continue
            
        # Check dependencies
        if not check_dependencies(step_key, year, month, base_dir, force=args.force):
            continue

        module = load_module(script_name)
        if not module:
            if step_key == "step1_nf": # Optional
                continue
            print(f"❌ Critical script missing: {script_name}")
            break
            
        success = run_step(step_key, module, year, month)
        if not success:
            print(f"⛔ Pipeline stopped due to failure in {step_key}")
            break
            
        # Manual Step Prompt after Atualiza_Entradas
        if step_key == "step3_update_entradas" and success:
            prompt_manual_step("Please open 'T_Entradas.xlsx', SAVE it (to recalculate formulas), and close it.")

if __name__ == "__main__":
    main()
