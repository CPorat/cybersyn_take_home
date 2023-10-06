### Cybersyn Take Home - Chris Porat

The dbt project has 5 schemas:
- `SRC`: source (raw) data
- `DEV_SNAPSHOTS`: snapshots of source data, to be able to reconstruct point in time queries
- `DEV_STAGING`: dbt staging models, minimal transformation and renaming/data-typing - selected from snapshots
- `DEV_INTERMEDIATE`: dbt intermediate models - where most of the transformations/heavy lifting occurs
- `DEV_REPORTING`: final, output models called for in the case doc (all views that point to int models)

### Part 1

#### 1. Cleaned Store Names
Output model: `iowa_liquor__store_cleaned.sql`

Related model(s):
- `int_iowa_liquor__cleaned_stores.sql`

If given more time, more manual mapping could be done. For the given timeline, I focused on:
- Standardizing spacing, capitalization, and remove non-needed characters
- Splitting store name to remove common store number variations
- Using case when statements for the most commonly seen store name variations

However, there are still outlier store names that did not follow the same pattern that would need to be handled for.

#### 2. Period Input Macro
Output model: `iowa_liquor__periods.sql`

Related model(s):
- `int_iowa_liquor__periods_week.sql`
- `int_iowa_liquor__periods_month.sql`
- `int_iowa_liquor__periods_quarter.sql`
- `int_iowa_liquor__periods_all.sql`

Related macro(s):
- ```aggregate__iowa_liquor_to_period```

The `aggregate__iowa_liquor_to_period` macro aggregates a table to a specified time period.

- It generates a date spine for the selected period to ensure no dates are missed.
- The base table is left joined to bring in the data.
- The data is grouped by the period start date and dimensions.
- Aggregates like total sales are calculated.
- The final aggregated table with a surrogate key is returned to allow joining to prior periods.

Arguments:

- table_name: The base table name to aggregate
- period: The time period to aggregate to, like month or quarter
- timestamp_field: The timestamp field to use to assign records to periods

Returns:

An aggregated table with a surrogate key and aggregated metrics like total sales summarized to the desired time period.

Important to note - the prior year period is identified using a date dimension from the `dbt_date` package in this macro, as well as a surrogate key, for ease of joins in the next step.
Up to my discretion, not sure I'd make this a macro for purposes of reusability vs. (relatively) one-off logic living in macros, but happy to talk through more live.

#### 3. YoY Change
Output model: `iowa_liquor__periods_yoy.sql`

Related model(s):
- `int_iowa_liquor__periods_week__yoy.sql`
- `int_iowa_liquor__periods_month__yoy.sql`
- `int_iowa_liquor__periods_quarter__yoy.sql`
- `int_iowa_liquor__periods_all__yoy.sql`

Related macro(s):
- ```calculate__yoy_change.sql```

The `calculate__yoy_change` macro calculates the year-over-year percentage change for a set of value fields.

- It joins the base table to itself, lining up the current and prior year records based on the match key.
- Then it calculates the yoy change as:

(current_value - prior_year_value) / prior_year_value

Arguments:

- table_name: The name of the base table to calculate over
- match_key: The field to match current and prior year records on
- date_field: The date field to use for the current year
- prior_year_date_field: The date field to use for the prior year
- label_fields: The fields to carry over verbatim from the base table
- value_fields: The numeric fields to calculate yoy change for

Returns:

A table with the label fields, current/prior year value fields, and yoy change fields.

### Part 2

#### 1. Incremental Model
Output model: `iowa_liquor__incremental.sql`

Related model(s):
- `int_iowa_liquor__incremental.sql`

This incremental model follows the steps outlined in the requirements document:

- It brings in data from 2 staging tables: `stg_iowa_liquor__sales_2018_2020` and `stg_iowa_liquor__sales_2019_2021`
- It unions the data from these 2 sources into a CTE called new_data
- It adds a qualify clause to new_data to only select the latest record for each invoice_item_number based on update_timestamp
- If this is an incremental run, it brings in the previous aggregated table as old_data
- It compares new_data and old_data to find new/updated rows in new_data and unchanged rows in old_data
- It unions these together to create a final aggregated table with all latest data
- If not incremental, it just selects from new_data as final
- Finally, it selects all records from the final CTE to populate the incremental model table

#### 2. Snapshot Model
Output model: `iowa_liquor__snapshot.sql`

Related model(s):
- `int_iowa_liquor__snapshot.sql`

This model is very simple, using dbt snapshots to allow for easy point in time queries. First the data from both sources is deduped,
using the max update_timestamp for each pk. Then we coalesce `dbt_valid_to` to a date in the distant future if it is null, as currently
valid rows will have a null `dbt_valid_to` date. We also calculate the version of the row using a window function which can be a nice to have
for business users to see how often a row has been updated over time.

To use this table for point in time data - say, for what the data looked like on 2023-03-01, you'd simply query:
```
select * from iowa_liquor__snapshot
where '2023-03-01' between dbt_valid_from and dbt_valid_to
```