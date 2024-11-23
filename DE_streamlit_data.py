import psycopg2 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from mpl_toolkits.mplot3d import Axes3D

# con = redshift_connector.connect(
#     database= 'olist-ecommerce',
#     host= 'dataengineering-workgroup.205930623637.us-east-1.redshift-serverless.amazonaws.com',
#     port= 5439,
#     user= 'username',
#     password= 'Password1'
# )
con =  psycopg2.connect(database="my_pgdb", 
                        user='postgres', password='1234',  
                        host='127.0.0.1', port='5432'
) 

def get_nearest_month_revenue():
    query = """
        WITH max_date AS (
            SELECT 
                EXTRACT(MONTH FROM MAX(order_purchase_timestamp)) AS max_month,
                EXTRACT(YEAR FROM MAX(order_purchase_timestamp)) AS max_year
            FROM orders
            WHERE order_approved_at IS NOT NULL
        )
        SELECT 
            SUM(op.payment_value) AS total_revenue,
            max_date.max_month AS month,
            max_date.max_year AS year,
            COUNT(op.payment_value) AS total_orders
        FROM orders o
        JOIN order_payments op ON o.order_id = op.order_id,
        max_date
        WHERE o.order_approved_at IS NOT NULL
        AND EXTRACT(MONTH FROM o.order_purchase_timestamp) = max_date.max_month
        AND EXTRACT(YEAR FROM o.order_purchase_timestamp) = max_date.max_year
        GROUP BY max_date.max_month, max_date.max_year;
    """
    data = pd.read_sql_query(query, con)
    return data


def get_city_order_nearest_year():
    query = """
        SELECT
            c.customer_city AS city,
            COUNT(o.order_id) AS order_count
        FROM
            orders o
        JOIN
            customers c ON o.customer_id = c.customer_id
        WHERE
            EXTRACT(YEAR FROM o.order_purchase_timestamp) = (
                SELECT EXTRACT(YEAR FROM MAX(order_purchase_timestamp))
                FROM orders
                WHERE order_approved_at IS NOT NULL
            )
        GROUP BY
            c.customer_city
        ORDER BY
            order_count DESC
        LIMIT 6;
    """
    data = pd.read_sql_query(query,con)
    return data

def top_selling_product():
    query = """
        SELECT EXTRACT(YEAR FROM MAX(order_approved_at)) AS year, p.product_category_name, COUNT(oi.product_id) AS no_product_sold,
                SUM(pa.payment_value) AS total_rev
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        JOIN orders o ON oi.order_id = o.order_id
        JOIN order_payments pa ON oi.order_id = pa.order_id
        WHERE order_status <> 'canceled' AND order_approved_at IS NOT NULL
        GROUP BY 2
        ORDER BY 3 DESC
        LIMIT 10;
    """
    data = pd.read_sql_query(query, con)
    return data

def count_repeat_sale():
    query = """
        SELECT COUNT(*)
        FROM(
            SELECT customer_state, customer_unique_id, COUNT(DISTINCT order_id)
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY 1,2
            HAVING COUNT(order_id) > 1
            ORDER BY 2 DESC)sub;
    """
    data = pd.read_sql_query(query, con)
    return data

def repeat_sale_rev():
    query = """
        WITH repeat_customers AS
        (SELECT SUM(payment_value) total_rev
        FROM order_payments p
        JOIN orders o ON p.order_id = o.order_id
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE customer_unique_id in(
            SELECT customer_unique_id
            FROM(
                SELECT customer_unique_id, COUNT(order_id)
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                GROUP BY 1
                HAVING COUNT(order_id) > 1
                ORDER BY 2 DESC)sub)),
        total_sales AS
            (SELECT SUM(payment_value) ovr_total_rev
            FROM order_payments)
        SELECT ROUND(total_rev/ovr_total_rev * 100) AS perc_sales_repeat_c
        FROM repeat_customers
        CROSS JOIN total_sales;
    """
    data = pd.read_sql_query(query, con)
    return data

def aov_payment_type():
    query = """
        SELECT DISTINCT pa.payment_type, (SUM(pa.payment_value)/COUNT(o.order_id))
            AS avg_order_val
        FROM orders o
        JOIN order_payments pa ON o.order_id = pa.order_id
        JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY 1
        ORDER BY 2 DESC;
    """
    data = pd.read_sql_query(query, con)
    return data

def revenue_growth():
    query = """
        SELECT 
            EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
            SUM(payment_value) AS revenue
        FROM orders o
        JOIN order_payments op ON o.order_id = op.order_id
        WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = (
            SELECT EXTRACT(YEAR FROM MAX(order_purchase_timestamp))
            FROM orders
        )
        GROUP BY EXTRACT(MONTH FROM order_purchase_timestamp)
        ORDER BY month;
    """
    data = pd.read_sql_query(query, con)
    return data

def order_status_distribution():
    query = """
        SELECT order_status, COUNT(order_id) AS total_orders
        FROM orders
        GROUP BY order_status
        ORDER BY total_orders DESC;
    """
    data = pd.read_sql_query(query, con)
    return data

nearest_month_data = get_nearest_month_revenue()
city_order_nearest_year = get_city_order_nearest_year()
top_product = top_selling_product()
month = nearest_month_data.at[0, 'month']
year = nearest_month_data.at[0, 'year']
revenue = nearest_month_data.at[0, 'total_revenue']
total_orders = nearest_month_data.at[0, 'total_orders']

month_year = f"{month}/{year}"

col1, col2 = st.columns(2)

with col1:
    st.markdown("<h2 style='text-align: center;'>Total Revenue</h2>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='text-align: center;'>{month_year}</h3>", unsafe_allow_html=True) 
    st.markdown(f"<h1 style='text-align: center; color: green; margin-bottom: 50px;'>${revenue:,.2f}</h1>", unsafe_allow_html=True)
    
with col2:
    st.markdown("<h2 style='text-align: center;'>Total Paid Orders</h2>", unsafe_allow_html=True)
    st.markdown(f"<h3 style='text-align: center;'>{month_year}</h3>", unsafe_allow_html=True)
    st.markdown(f"<h1 style='text-align: center; color: blue; margin-bottom: 50px;'>{total_orders}</h1>", unsafe_allow_html=True)
    
col1, col2 = st.columns(2)
total_repeat = count_repeat_sale().at[0, 'count']
perc_repeat = repeat_sale_rev().at[0, 'perc_sales_repeat_c']

with col1:
    st.markdown("<h3 style='text-align: center;'>Total Repeat Customers</h3>", unsafe_allow_html=True) 
    st.markdown(f"<h2 style='text-align: center; color: yellow;'>{total_repeat}</h2>", unsafe_allow_html=True)
    
with col2:
    st.markdown("<h3 style='text-align: center;'>Percent Repeat Revenue</h3>", unsafe_allow_html=True)
    st.markdown(f"<h2 style='text-align: center; color: yellow;'>{perc_repeat}%</h2>", unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)    

st.markdown("<h2 style='text-align: center; border: 25px'>Data Visualization</h2>", unsafe_allow_html=True)


fig, ax = plt.subplots(figsize=(11, 6))
fig.patch.set_facecolor('#0e1117')
ax.set_facecolor('#0e1117')

bars = ax.bar(city_order_nearest_year['city'], city_order_nearest_year['order_count'], color='#fad744', alpha=0.8)
ax.set_title("Top 6 Cities by Order Count in Latest Year", color='#f5f5dc', pad= 40, fontsize= 25)
ax.set_xlabel("City", color='#f5f5dc', fontweight='bold', labelpad= 25, loc='center')
ax.set_ylabel("Number of Orders", color='#f5f5dc', fontweight= 'bold', labelpad=25)

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, height, f'{height}', 
            ha='center', va='bottom', color='#f5f5dc', fontsize=12)

plt.xticks(rotation=25, ha='right', color='#f5f5dc',fontsize=10) 
plt.yticks(color='#f5f5dc', fontsize=10)
plt.subplots_adjust(top=1.5, bottom=0.2)

plt.tight_layout()
st.pyplot(plt)

st.markdown("<hr>", unsafe_allow_html=True) 

fig, ax = plt.subplots(figsize=(11,6))
fig.patch.set_facecolor('#0e1117')
ax.set_facecolor('#0e1117')

bars = ax.bar(top_product['product_category_name'], top_product['no_product_sold'], color='#fad744', alpha=0.8)
ax.set_title("Top 10 best-selling products in the most recent year", color='#f5f5dc', pad= 25, fontsize=25)
ax.set_xlabel("Product", color='#f5f5dc', fontweight='bold', labelpad= 25, loc='center')
ax.set_ylabel("Number of products sold", color='#f5f5dc', fontweight= 'bold', labelpad=25)

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, height, f'{height}', 
            ha='center', va='bottom', color='#f5f5dc', fontsize=12)

plt.xticks(rotation=25, ha='right', color='#f5f5dc',fontsize=10)  
plt.yticks(color='#f5f5dc', fontsize=10)
plt.subplots_adjust(top=1.5, bottom=0.2)

plt.tight_layout()
st.pyplot(plt)

st.markdown("<hr>", unsafe_allow_html=True)

print(aov_payment_type())
aov = aov_payment_type()
fig, ax = plt.subplots(figsize=(5,5))
fig.patch.set_facecolor('#0e1117')
wedges, _, autotexts = ax.pie(aov['avg_order_val'], labels=None,autopct='%1.1f%%',startangle=90)

ax.set_title('Average Order Value By Payment Type', color='#f5f5dc', pad=0, fontsize=15)
ax.axis('equal')
ax.legend(wedges,aov['payment_type'], title="Type",loc="center left", bbox_to_anchor=(1,0,0.5,1))
plt.tight_layout()
st.pyplot(plt)

st.markdown("<hr>", unsafe_allow_html=True)
revenue_data = revenue_growth()
fig, ax = plt.subplots(figsize=(10, 6))
fig.patch.set_facecolor('#0e1117')
ax.set_facecolor('#0e1117')

ax.plot(revenue_data['month'], revenue_data['revenue'], marker='o', linestyle='-', color='cyan')
ax.set_title("Monthly Revenue Growth in Latest Year", color='#f5f5dc', fontsize=20)
ax.set_xlabel("Month", color='#f5f5dc', fontsize=12)
ax.set_ylabel("Revenue ($)", color='#f5f5dc', fontsize=12)

ax.tick_params(colors='#f5f5dc')
plt.xticks(np.arange(1, 13), fontsize=10, color='#f5f5dc')
plt.yticks(fontsize=10, color='#f5f5dc')

plt.grid(visible=True, color='#444444', linestyle='--', linewidth=0.5)
plt.tight_layout()
st.pyplot(fig)

st.markdown("<hr>", unsafe_allow_html=True)

order_status_data = order_status_distribution()


fig = plt.figure(figsize=(10, 7))
fig.patch.set_facecolor('#0e1117')
ax = fig.add_subplot(111, projection='3d')
ax.set_facecolor('#0e1117')


x = np.arange(len(order_status_data))  
y = [0] * len(order_status_data)       
z = [0] * len(order_status_data)       
dx = np.ones(len(order_status_data))   
dy = np.ones(len(order_status_data))   
dz = order_status_data['total_orders'] 


colors = plt.cm.inferno(np.linspace(0, 1, len(order_status_data)))
ax.bar3d(x, y, z, dx, dy, dz, color=colors, alpha=0.9)

ax.set_zlim(0, max(dz) * 1.2)

ax.set_xticks(x)
ax.set_xticklabels(order_status_data['order_status'], rotation=45, ha='right', fontsize=10, color='#f5f5dc')
ax.set_yticks([])
ax.set_zticks(np.arange(0, max(dz) + 1, int(max(dz) / 5)))
ax.tick_params(colors='#f5f5dc')

for i in range(len(dz)):
    ax.text(x[i], y[i], dz[i] + max(dz) * 0.05, f'{dz[i]}', color='#f5f5dc', ha='center', fontsize=10)

ax.set_title("Order Status Distribution", color='#f5f5dc', fontsize=16, pad=20)
ax.set_xlabel("Order Status", color='#f5f5dc', fontsize=12, labelpad=15)
ax.set_ylabel("")
ax.set_zlabel("Total Orders", color='#f5f5dc', fontsize=12, labelpad=10)

plt.tight_layout()
st.pyplot(fig)

con.close() 