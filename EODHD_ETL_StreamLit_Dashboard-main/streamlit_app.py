import streamlit as st
import altair as alt
import pandas as pd
import helper_functions

# --- Streamlit App ---

st.title("Bond Data Dashboard")

# Table Selection (Inferred from ETL Code)
table_names = ["de10_cdw_uk_1y_gbond", "de10_cdw_uk_2y_gbond", "de10_cdw_uk_3y_gbond",
               "de10_cdw_uk_5y_gbond", "de10_cdw_uk_10y_gbond", "de10_cdw_uk_30y_gbond",
               "de10_cdw_us_1y_gbond", "de10_cdw_us_2y_gbond", "de10_cdw_us_3y_gbond",
               "de10_cdw_us_5y_gbond", "de10_cdw_us_10y_gbond", "de10_cdw_us_30y_gbond",
               "de10_cdw_de_1y_gbond", "de10_cdw_de_2y_gbond", "de10_cdw_de_5y_gbond",
               "de10_cdw_de_10y_gbond", "de10_cdw_de_30y_gbond"]

# More user-friendly names for display
table_display_names = {
    "de10_cdw_uk_1y_gbond": "UK 1Y GBOND",
    "de10_cdw_uk_2y_gbond": "UK 2Y GBOND",
    "de10_cdw_uk_3y_gbond": "UK 3Y GBOND",
    "de10_cdw_uk_5y_gbond": "UK 5Y GBOND",
    "de10_cdw_uk_10y_gbond": "UK 10Y GBOND",
    "de10_cdw_uk_30y_gbond": "UK 30Y GBOND",
    "de10_cdw_us_1y_gbond": "US 1Y GBOND",
    "de10_cdw_us_2y_gbond": "US 2Y GBOND",
    "de10_cdw_us_3y_gbond": "US 3Y GBOND",
    "de10_cdw_us_5y_gbond": "US 5Y GBOND",
    "de10_cdw_us_10y_gbond": "US 10Y GBOND",
    "de10_cdw_us_30y_gbond": "US 30Y GBOND",
    "de10_cdw_de_1y_gbond": "DE 1Y GBOND",
    "de10_cdw_de_2y_gbond": "DE 2Y GBOND",
    "de10_cdw_de_5y_gbond": "DE 5Y GBOND",
    "de10_cdw_de_10y_gbond": "DE 10Y GBOND",
    "de10_cdw_de_30y_gbond": "DE 30Y GBOND",
}

# Corresponding symbols in summary table
table_summary_symbols = {
    "de10_cdw_uk_1y_gbond": "UK1Y.GBOND",
    "de10_cdw_uk_2y_gbond": "UK2Y.GBOND",
    "de10_cdw_uk_3y_gbond": "UK3Y.GBOND",
    "de10_cdw_uk_5y_gbond": "UK5Y.GBOND",
    "de10_cdw_uk_10y_gbond": "UK10Y.GBOND",
    "de10_cdw_uk_30y_gbond": "UK30Y.GBOND",
    "de10_cdw_us_1y_gbond": "US1Y.GBOND",
    "de10_cdw_us_2y_gbond": "US2Y.GBOND",
    "de10_cdw_us_3y_gbond": "US3Y.GBOND",
    "de10_cdw_us_5y_gbond": "US5Y.GBOND",
    "de10_cdw_us_10y_gbond": "US10Y.GBOND",
    "de10_cdw_us_30y_gbond": "US30Y.GBOND",
    "de10_cdw_de_1y_gbond": "DE1Y.GBOND",
    "de10_cdw_de_2y_gbond": "DE2Y.GBOND",
    "de10_cdw_de_5y_gbond": "DE5Y.GBOND",
    "de10_cdw_de_10y_gbond": "DE10Y.GBOND",
    "de10_cdw_de_30y_gbond": "DE30Y.GBOND",
}

# Table Selection (Organized with Expanders)
country_groups = {}
for table_name, display_name in table_display_names.items():
    country = table_name.split('_')[2].upper()
    if country not in country_groups:
        country_groups[country] = []
    country_groups[country].append(display_name)

selected_chart_tables = []

# Create columns for bond selections
col1, col2, col3 = st.columns(3)

# Bond Selection for Chart
with col1:
    st.subheader("UK Bonds")
    for table in country_groups["UK"]:
        if st.checkbox(table, key=f"chart_{table}"):
            selected_chart_tables.append([k for k, v in table_display_names.items() if v == table][0])

with col2:
    st.subheader("US Bonds")
    for table in country_groups["US"]:
        if st.checkbox(table, key=f"chart_{table}"):
            selected_chart_tables.append([k for k, v in table_display_names.items() if v == table][0])

with col3:
    st.subheader("DE Bonds")
    for table in country_groups["DE"]:
        if st.checkbox(table, key=f"chart_{table}"):
            selected_chart_tables.append([k for k, v in table_display_names.items() if v == table][0])

# Data Loading and Display for Chart
if selected_chart_tables:
    combined_df = pd.DataFrame()

    for selected_table_name in selected_chart_tables:
        try:
            df = helper_functions.get_bond_data(selected_table_name)
        except Exception as e:
            st.error(f"Error fetching data for {table_display_names[selected_table_name]}: {e}")
            continue
        
        df['Bond'] = table_display_names[selected_table_name]  # Safe because we checked for valid names earlier
        combined_df = pd.concat([combined_df, df])

    if not combined_df.empty and 'adjusted_close' in combined_df.columns:
        st.subheader("Adjusted Close Over Time")
        chart = alt.Chart(combined_df).mark_line().encode(
            x='date',
            y='adjusted_close',
            color='Bond:N',
            tooltip=['date', 'adjusted_close', 'Bond']
        ).properties(
            title="Adjusted Close for Multiple Bonds"
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

        # Fetch and display summary data for selected bonds
        all_summary_df = pd.DataFrame()
        for selected_table_name in selected_chart_tables:
            try:
                summary_symbol = table_summary_symbols[selected_table_name]
                summary_df = helper_functions.get_bond_summary_data(summary_symbol)
            except Exception as e:
                st.error(f"Error fetching summary data for {table_display_names[selected_table_name]}: {e}")
                continue

            summary_df['Bond'] = table_display_names.get(selected_table_name, "Unknown Bond")
            all_summary_df = pd.concat([all_summary_df, summary_df])

        if not all_summary_df.empty:
            st.subheader("Summary Data for Selected Bonds")
            st.dataframe(all_summary_df)

            # Key for Summary Data Columns
            st.markdown("""
            **Key for Summary Data Columns:**
            - **Column 1**: num_yield_reports is the number of available yield observations in the db.
            - **Column 2**: maX = the average of the X latest yield observations.
            - **Column 3**: diff_maX = the latest yield - the maX.
            - **Column N**: Explanation of what data column N represents.
            """)