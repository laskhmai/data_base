Hi Govindaraj,

I checked the module code for functionapp9.

The app_settings are defined only for the primary function app. The deployment slot resource does not currently include or manage app_settings.

That is why the primary app shows dotnet-isolated, while the slot still has the previous value.