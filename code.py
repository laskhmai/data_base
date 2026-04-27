placeholders = ",".join(["?" for _ in batch])
sql = f"DELETE FROM {table} WHERE {key} IN ({placeholders})"
conn.execute(sql, batch)
conn.commit()
print(f"Deleted batch of {len(batch)} records")