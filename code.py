def component_comment(cpu_idx: int, mem_idx: int, conn_idx: int, current_idx: int) -> str:
    components = {"CPU": cpu_idx, "Memory": mem_idx, "Connections": conn_idx}

    intensive, underutilized, optimal = [], [], []
    for name, val in components.items():
        if val > current_idx:
            intensive.append(name)      # needs bigger SKU
        elif val < current_idx:
            underutilized.append(name)  # can reduce SKU
        else:
            optimal.append(name)        # keep same SKU

    parts = []
    if intensive:
        parts.append(", ".join(intensive) + " Intensive")
    if underutilized:
        parts.append(", ".join(underutilized) + " Underutilized")
    if optimal:
        parts.append(", ".join(optimal) + " Optimal Usage")
    return " ; ".join(parts)