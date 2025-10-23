# AUR Tableau View Generator

A Python-based ETL utility for automating the creation and maintenance of **PostgreSQL views** used in **Tableau dashboards** for the **Arab University Rankings (AUR)**.

## Overview

This utility automates the generation of Tableau-ready PostgreSQL views by:
- Creating year-based **key–value pair (KVP)** views from raw ranking data
- Appending the new yearly views to an existing **union view** for cumulative analysis
- Removing the need for manual SQL maintenance each year

## Prerequisites

- Python 3.9+
- PostgreSQL database access
- Required Python packages:
  ```bash
  pip install pandas sqlalchemy psycopg[binary] python-dotenv pyyaml
Configuration
Environment Variables (.env)
Create a .env file in the project root containing your PostgreSQL credentials:

pgsql
Copy code
PG_Host=your_host
PG_Port=5432
PG_User=your_user
PG_Password=your_password
PG_DB=your_database
⚠️ Note: .env is already excluded in .gitignore — do not commit it to the repository.

YAML Configuration (aur_config.yml)
The pipeline reads configuration settings from aur_config.yml, which defines the source and target view details, along with constants for ranking context.

Example:

yaml
Copy code
job:
  year: 2026

  source:
    schema: wur
    view: rnk_arab_2026_latest_vw

  target:
    schema: tableau_data
    view: kvp_arab_2026_vw

  constants:
    ranking: "AUR"
    ranking_detail: "Overall"
File Structure
bash
Copy code
AUR_tableau/
├── Generatetableauview.py   # Main script
├── aur_config.yml            # Configuration file
├── .env                      # Environment variables
├── requirements.in            # Base dependencies
├── requirements.txt           # Pinned dependencies             
└── README.md                  # Project overview (this file)
Usage
Run the main script to generate or refresh Tableau-compatible PostgreSQL views:

bash
Copy code
python Generatetableauview.py
What It Does
Establishes connection to the configured PostgreSQL database.

Creates or updates the yearly view (e.g., kvp_arab_2026_vw).

Appends the new view to the union view (kvp_arab_rankings_vw_test), if not already included.

Example Output
pgsql
Copy code
Database connection established.
View tableau_data.kvp_arab_2026_vw created/refreshed successfully.
Added kvp_arab_2026_vw to tableau_data.kvp_arab_rankings_vw_test.
If the specified yearly view does not exist:

pgsql
Copy code
View tableau_data.kvp_arab_2026_vw not found — skipping update.
Key Components
Function	Description
create_tableau_view()	Generates a new year-specific KVP view based on configuration
append_year_to_union_view()	Adds the new view to the existing cumulative union view
_kvp_rows_fixed()	Defines static mappings of metrics and field types for transformation
_values_sql()	Converts mappings into SQL syntax for PostgreSQL view creation

Example Use Case
When new Arab University Rankings data for 2026 becomes available:

Update aur_config.yml with the new year and source view name.

Run the script to create kvp_arab_2026_vw.

The union view (kvp_arab_rankings_vw_test) automatically includes the 2026 dataset, making it visible in Tableau.


Maintenance
Configuration changes (e.g., schema, year) are managed via aur_config.yml.

The .env file should be updated for any credential or host changes.

Yearly updates involve changing only the year and source.view entries.

The script is designed for idempotent execution — running it multiple times for the same year does not cause duplication.

Database Artifacts
Artifact	                                    |               Description
tableau_data.kvp_arab_{year}_vw	                |       Year-specific KVP view generated for Tableau
tableau_data.kvp_arab_rankings_vw_test	        |       Master union view aggregating multiple years

Contributing
Fork the repository.

Create a new feature branch.

Implement your changes.

Run and test the pipeline.

Submit a pull request.

Ownership
Team: Data & Analytics
Organization: THE World Universities Insights Ltd.
Confidential: Internal use only.

