import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum
import snowflake.permissions as permission
from sys import exit

st.set_page_config(layout="wide")
session = get_active_session()

def inner_join(df_sharethis, df_client, sharethis_col_name, client_col_name):
    # result = session.sql(f"SELECT * FROM {df_sharethis} INNER JOIN {df_client} ON {df_sharethis}.{sharethis_col_name} = {df_client}.{client_col_name}")
    # st.subheader(f"SELECT * FROM {df_sharethis} as t1 INNER JOIN {df_client} as t2 ON t1.{sharethis_col_name} = t2.{client_col_name}")
    # st.subheader(result)
    # st.table(result)
    df_sharethis = session.sql(f"SELECT * FROM SHARETHIS_DATA")
    df_client = session.sql(f"SELECT * FROM {client_table}")
    result = df_sharethis.join(df_client, df_sharethis[sharethis_col_name] == df_client[client_col_name])
    st.subheader(f"{result.count() * 100 // df_client.count()}% of your customers match up with our Database record!")
    st.table(result)




def load_app(orders_table, site_recovery_table,  client_table):
    with st.spinner("Loading lead time, order status, and supplier performance. Please wait..."):
        # df = session.sql(f"SELECT t1.order_id,t2.ship_order_id,t1.material_name,t1.supplier_name, t1.quantity, t1.cost, t2.status, t2.lat, t2.lon FROM {orders_table} as t1 INNER JOIN MFG_SHIPPING as t2 ON t2.ORDER_ID = t1.ORDER_ID ORDER BY t1.order_id")
        # df_order_status = df.group_by('status').agg(count_distinct('order_id').as_('TOTAL RECORDS')).order_by('status').to_pandas()

        # df_cal_lead_time = session.sql(f"SELECT t1.order_id,t2.ship_order_id,t1.material_name,t1.supplier_name,t1.quantity,t1.cost,t2.status,t2.lat,t2.lon,cal_lead_time(t1.process_supply_day,t2.duration,t2.recovery_days) as lead_time FROM {orders_table} as t1 INNER JOIN (SELECT order_id, ship_order_id, status, duration, MFG_SHIPPING.lat, MFG_SHIPPING.lon, IFF(srt.recovery_weeks * 7::int = srt.recovery_weeks * 7,srt.recovery_weeks * 7,0) as recovery_days from MFG_SHIPPING LEFT OUTER JOIN {site_recovery_table} as srt ON MFG_SHIPPING.lon = srt.lon AND MFG_SHIPPING.lat = srt.lat) as t2 ON t2.ORDER_ID = t1.ORDER_ID ORDER BY t1.order_id")
        # st.subheader(f"SELECT t1.order_id,t2.ship_order_id,t1.material_name,t1.supplier_name,t1.quantity,t1.cost,t2.status,t2.lat,t2.lon,cal_lead_time(t1.process_supply_day,t2.duration,t2.recovery_days) as lead_time FROM {orders_table} as t1 INNER JOIN (SELECT order_id, ship_order_id, status, duration, MFG_SHIPPING.lat, MFG_SHIPPING.lon, IFF(srt.recovery_weeks * 7::int = srt.recovery_weeks * 7,srt.recovery_weeks * 7,0) as recovery_days from MFG_SHIPPING LEFT OUTER JOIN {site_recovery_table} as srt ON MFG_SHIPPING.lon = srt.lon AND MFG_SHIPPING.lat = srt.lat) as t2 ON t2.ORDER_ID = t1.ORDER_ID ORDER BY t1.order_id")
        # df_supplier_perf = df_cal_lead_time.group_by('supplier_name').agg(sum(col('lead_time')).as_('TOTAL LEAD TIME')).sort('TOTAL LEAD TIME', ascending=True).limit(20).to_pandas()
        # df_lead_time = df_cal_lead_time.select('order_id','lead_time').sort('order_id', ascending=True).to_pandas()
        
        df_sharethis = session.sql(f"SELECT * FROM SHARETHIS_DATA")
        df_client = session.sql(f"SELECT * FROM {client_table}")

        with st.container():
            # col1,col2,col3 = st.columns(3,gap='small')

            # with col1:
                # text1 = st.text_area("Try to see how ShareThis' database match up with your customers' profile.")
                st.text_input(label="Enter Table Name", value="Default Input Holder", max_chars=None, key=None, type="default", help=None, autocomplete=None, on_change=None, args=None, kwargs=None, placeholder=None, disabled=False, label_visibility="visible")
                client_col_name = st.text_input(label="Enter ID Join Key Name", value="Default Input Holder", max_chars=None, key=None, type="default", help=None, autocomplete=None, on_change=None, args=None, kwargs=None, placeholder=None, disabled=False, label_visibility="visible")
                sharethis_col_name = st.text_input(label="Enter ID Join Key Type", value="Default Input Holder", max_chars=None, key=None, type="default", help=None, autocomplete=None, on_change=None, args=None, kwargs=None, placeholder=None, disabled=False, label_visibility="visible")
                st.button(label="Submit", key=None, help=None, on_click=inner_join, args=("SHARETHIS_DATA", client_table, sharethis_col_name, client_col_name), kwargs=None, type="secondary", disabled=False, use_container_width=False)

                # st.table(df_sharethis)
                # st.write("---")
                # st.subheader(site_recovery_table)
                # st.table(df_client)


            # with col2:
            #     # Display Lead Time Status chart
            #     st.subheader("Lead Time Status")
            #     lead_time_base = alt.Chart(df_lead_time).encode(alt.X("ORDER_ID", title="ORDER ID", sort=None))
            #     lead_time_base_bars = lead_time_base.mark_bar().encode(
            #         color=alt.value("#249DC9"),
            #         y=alt.Y("LEAD_TIME", title="LEAD TIME DAYS")
            #     )
            #     line = alt.Chart(pd.DataFrame({'y': [60]})).mark_rule(color='rgb(249,158,54)').encode(y='y')
            #     lead_time_chart = alt.layer(lead_time_base_bars)
            #     st.altair_chart(lead_time_chart + line, use_container_width=True)
        
            # def color_lead_time(val):
            #     return f'background-color: rgb(249,158,54)'
        
            # with col3:
            #     # Underlying Data
            #     st.subheader("Orders with Lead Time Status >= 60days")
            #     df_lead_time_60_days = df_cal_lead_time.select('lead_time','order_id','ship_order_id','material_name','supplier_name','quantity','cost').filter(col('lead_time') > 60).sort('lead_time').to_pandas()
            #     df_lead_time_60_days['LEAD_TIME'] = df_lead_time_60_days['LEAD_TIME'].astype('int')
            #     st.dataframe(df_lead_time_60_days.style.applymap(color_lead_time, subset=['LEAD_TIME']))
      
        # with st.container():
        #     col1,col2 = st.columns(2,gap='small')
        #     with col1:
        #         # Display Supplier Performance
        #         st.subheader("Supplier Performance")
        #         supplier_perf_base = alt.Chart(df_supplier_perf).encode(alt.X("SUPPLIER_NAME:N", title="SUPPLIER NAME", sort=None))
        #         supplier_perf_base_bars = supplier_perf_base.mark_bar().encode(
        #             color=alt.value("#249DC9"),
        #             y=alt.Y("TOTAL LEAD TIME", title="TOTAL LEAD TIME")
        #         )
        #         supplier_perf_chart = alt.layer(supplier_perf_base_bars)
        #         st.altair_chart(supplier_perf_chart, use_container_width=True)
                
        #         # Underlying Data
        #         # st.subheader("Underlying Data")
        #         # st.dataframe(df_lead_time)
        
        #     with col2:
        #         # Display Purchase Order Status 
        #         st.subheader("Purchase Order Status")
        #         order_status_base = alt.Chart(df_order_status).encode(alt.X("STATUS", sort=['Order_confirmed','Shipped','In_Transit','Out_for_delivery','Delivered']))
        #         order_status_base_bars = order_status_base.mark_bar().encode(
        #             color=alt.value("#249DC9"),
        #             y=alt.Y("TOTAL RECORDS", title="TOTAL RECORDS")
        #         )
        #         order_status_chart = alt.layer(order_status_base_bars)
        #         st.altair_chart(order_status_chart, use_container_width=True)
                
        #         # Underlying Data
        #         # st.subheader("Underlying Data")
        #         # st.dataframe(df_order_status)

# orders_reference_associations = permission.get_reference_associations("order_table")
# if len(orders_reference_associations) == 0:
#     permission.request_reference("order_table")
#     exit(0)

# site_recovery_reference_associations = permission.get_reference_associations("site_recovery_table")
# if len(site_recovery_reference_associations) == 0:
#     permission.request_reference("site_recovery_table")
#     exit(0)

client_reference_associations = permission.get_reference_associations("client_table")
if len(client_reference_associations) == 0:
    permission.request_reference("client_table")
    exit(0)

st.title("Try to test out the overlapping rate of your customer with ShareThis database")
orders_table = "reference('order_table')"
site_recovery_table = "reference('site_recovery_table')"
client_table = "reference('client_table')"
load_app(orders_table,site_recovery_table,client_table)
