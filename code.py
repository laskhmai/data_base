def calculate_efficiency(data: pd.DataFrame, actual_sku: str, recommended_sku: str, specs: dict) -> tuple:
    avg_cpu  = _safe_mean(data["CpuAvg"])             if "CpuAvg"             in data.columns else 0.0
    max_cpu  = _safe_max(data["CpuMax"])              if "CpuMax"             in data.columns else 0.0
    avg_mem  = _safe_mean(data["MemResidentAvgPct"])  if "MemResidentAvgPct"  in data.columns else 0.0
    max_mem  = _safe_max(data["MemResidentMaxPct"])   if "MemResidentMaxPct"  in data.columns else 0.0
    avg_conn = _safe_mean(data["ConnUtilizationPct"]) if "ConnUtilizationPct" in data.columns else 0.0

    current_efficiency = json.dumps({
        "CpuAvgPct": round(avg_cpu,  2),
        "CpuMaxPct": round(max_cpu,  2),
        "MemAvgPct": round(avg_mem,  2),
        "MemMaxPct": round(max_mem,  2),
        "ConnPct":   round(avg_conn, 2),
    })

    current_ram  = specs.get(actual_sku,      {}).get("RAM_GB",          0.0)
    rec_ram      = specs.get(recommended_sku, {}).get("RAM_GB",          0.0)
    current_conn = specs.get(actual_sku,      {}).get("ConnectionLimit",  0.0)
    rec_conn     = specs.get(recommended_sku, {}).get("ConnectionLimit",  0.0)
    current_vcpu = specs.get(actual_sku,      {}).get("vCores",           0)
    rec_vcpu     = specs.get(recommended_sku, {}).get("vCores",           0)

    proj_mem     = round((avg_mem  * current_ram  / rec_ram)  if rec_ram  > 0 else 0.0,     2)
    proj_conn    = round((avg_conn * current_conn / rec_conn) if rec_conn > 0 else 0.0,     2)
    proj_cpu_avg = round((avg_cpu  * current_vcpu / rec_vcpu) if rec_vcpu > 0 else avg_cpu, 2)
    proj_cpu_max = round((max_cpu  * current_vcpu / rec_vcpu) if rec_vcpu > 0 else max_cpu, 2)

    within_efficiency = json.dumps({
        "ProjectedCpuAvgPct": proj_cpu_avg,
        "ProjectedCpuMaxPct": proj_cpu_max,
        "ProjectedMemPct":    proj_mem,
        "ProjectedConnPct":   proj_conn,
        "RecommendedSku":     recommended_sku,
    })

    return current_efficiency, within_efficiency