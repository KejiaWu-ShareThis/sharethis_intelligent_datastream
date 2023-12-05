import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum
import snowflake.permissions as permission
from sys import exit

st.set_page_config(layout="wide")
session = get_active_session()
# temp = None

def inner_join(df_sharethis, df_client, sharethis_col_name, client_col_name):
    df_client = session.sql(f"SELECT * FROM {client_table}")
    df_sharethis = session.sql(f"SELECT * FROM SHARETHIS_DATA")
    result_table = df_client.join(df_sharethis, df_client[client_col_name] == df_sharethis[sharethis_col_name])
    st.subheader(f"{result_table.count() * 100 // df_client.count()}% of your customers match up with our Database record!")
    st.table(result_table)

def load_app(client_table):
    with st.spinner("Loading lead time, order status, and supplier performance. Please wait..."):
        # df_sharethis = session.sql(f"SELECT * FROM SHARETHIS_DATA")
        # df_client = session.sql(f"SELECT * FROM {client_table}")

        with st.container():
                st.text_input(label="Enter Table Name", value="Default Input Holder", max_chars=None, key=None, type="default", help=None, autocomplete=None, on_change=None, args=None, kwargs=None, placeholder=None, disabled=False, label_visibility="visible")
                client_col_name = st.text_input(label="Enter ID Join Key Name", value="Default Input Holder", max_chars=None, key=None, type="default", help=None, autocomplete=None, on_change=None, args=None, kwargs=None, placeholder=None, disabled=False, label_visibility="visible")
                sharethis_col_name = st.text_input(label="Enter ID Join Key Type", value="Default Input Holder", max_chars=None, key=None, type="default", help=None, autocomplete=None, on_change=None, args=None, kwargs=None, placeholder=None, disabled=False, label_visibility="visible")
                st.button(label="Submit", key=None, help=None, on_click=inner_join, args=("SHARETHIS_DATA", client_table, sharethis_col_name, client_col_name), kwargs=None, type="secondary", disabled=False, use_container_width=False)

client_reference_associations = permission.get_reference_associations("client_table")
if len(client_reference_associations) == 0:
    permission.request_reference("client_table")
    exit(0)

st.title("Try to test out the overlapping rate of your customer with ShareThis database")
client_table = "reference('client_table')"
load_app(client_table)
