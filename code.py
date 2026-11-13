Hi Nate,

I forwarded the email thread for your review. The application team needs the Service Bus connection string for their Function Apps, but they cannot view or regenerate the keys in Azure Portal due to limited access.

I reviewed their repository and the modules and found:

* Their Service Bus configuration references module version 14.2.1.
* The Service Bus module’s output.tf exposes default-authorization-rule-primary-connection-string and default-authorization-rule-secondary-connection-string, referencing the namespace’s default connection strings.
* The Function App module accepts app-settings and merges them with the Dynatrace settings.
* The Function App entries reviewed in their locals.tf have usr-app-settings = {}. I have not yet confirmed whether their function.tf adds the Service Bus output when passing settings to the module.

Could you please review and advise whether we can help them consume the existing outputs through Terraform, or whether CPT needs to assist with the permissions? We have not identified a module failure so far.

Thanks,
Jayanth