I am running `SUBS_Cost_mgmt_Focus_SQL_Test.ipynb` locally on my company laptop.

The existing `CERT_PATH` points to my colleague's laptop, and I do not have that certificate file.

Please:

1. Check whether Python's `truststore` package is installed and whether my Windows certificate store contains the required trusted corporate CA certificates.
2. Review how `requests` uses the `verify=CERT_PATH` setting in the duplicate notebook.
3. If the corporate certificates are already trusted by Windows, configure the duplicate notebook to use secure Windows certificate-store verification without requiring my colleague's certificate path.
4. If the required certificates are missing, explain which approved certificate or CA bundle I need to obtain from our IT team.
5. Do not use `verify=False` or disable SSL verification.
6. Do not modify the original notebook.
7. Do not change any SQL tables or upload files to Blob Storage.
8. Do not execute Azure API requests yet.
9. Do not expose any credentials or tokens.

Make only the necessary certificate-related changes in the duplicate notebook and show me the changes before execution.