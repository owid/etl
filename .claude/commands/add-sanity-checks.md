# Add Sanity Checks

Can you add data sanity checks to <% if $ARGUMENTS %>the ETL step at `etl/steps/data/garden/$ARGUMENTS.py`<% else %>this ETL step<% endif %> based on the validation patterns and best practices outlined in the CLAUDE.md file? 

<% if $ARGUMENTS %>
Please work on the step located at: `etl/steps/data/garden/$ARGUMENTS.py` and its corresponding metadata file `etl/steps/data/garden/$ARGUMENTS.meta.yml` if it exists.
<% endif %>

Please follow these requirements:
- Add comprehensive validation functions following the patterns in CLAUDE.md
- Include basic data integrity, value range validation, temporal consistency, and domain-specific checks
- Use the metadata file (*.meta.yml) to inform validation logic where available
- Integrate validation calls into the main run() function BEFORE the .format() call
- Add structured logging for clear visibility into data quality issues
- Follow the import organization and code style conventions

Focus on creating validation functions that are tailored to the specific data type and domain of this ETL step.