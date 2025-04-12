#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pandas matplotlib seaborn azure-storage-blob pyodbc')


# In[9]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style = 'whitegrid')

df =pd.read_csv("sales_data.csv")
df.head()


# In[10]:


print(df.shape)


# In[11]:


print(df.dtypes)


# In[13]:


print(df.isnull().sum())


# In[15]:


print(df.duplicated().sum())


# In[22]:


df['sale_date'] = pd.to_datetime(df['sale_date'], errors = 'coerce')
print(df['sale_date'].dtype)
print(df['sale_date'].isnull().sum())


# In[18]:


df = df.drop_duplicates()


# In[19]:


df = df.sort_values(by = 'sale_date')


# In[23]:


df['year_month'] = df['sale_date'].dt.to_period('M')
df['order_month'] = df['sale_date'].dt.strftime('%b-%y')

df.head()


# In[25]:


total_revenue = df['sale_amount'].sum()
print('Total Revenue',round(total_revenue,2))


# In[26]:


avg_order_value =df['sale_amount'].mean()
print('Average Order Value',round(avg_order_value,2))


# In[28]:


top_products = df.groupby('product_name')['sale_amount'].sum().sort_values(ascending=False).head(5)
print(top_products)


# In[29]:


sale_by_region = df.groupby('region')['sale_amount'].sum().sort_values(ascending = False)
print(sale_by_region)


# In[30]:


monthly_revenue = df.groupby('year_month')['sale_amount'].sum()
print(monthly_revenue)


# In[70]:


import matplotlib.pyplot as plt

monthly_revenue.plot(kind = 'line',marker = 'o',figsize = (10,5),color='teal')
plt.title('Monthly Revenue Trend')
plt.xlabel('Month')
plt.ylabel('Revenue')
plt.grid(True)
plt.xticks(rotation = 45)
plt.tight_layout()
plt.savefig("Monthly Revenue Trend.png")
plt.show()


# In[71]:


top_products.plot(kind='bar',color='skyblue',figsize=(8,4))
plt.title("Top 5 Products By Revenue")
plt.ylabel("Revenue")
plt.xticks(rotation = 45)
plt.tight_layout()
plt.savefig("Top 5 Products By Revenue.png")
plt.show()


# In[72]:


sale_by_region.plot(kind = 'pie',autopct = '%1.1f%%',figsize=(6,6),startangle=90)
plt.title("Sales Distribution by Region")
plt.ylabel('')
plt.tight_layout()
plt.savefig("Sales Distribution by Region.png")
plt.show()


# In[73]:


from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

# Replace these with your actual values
account_name = 'salesdata2025advika'
container_name = 'salesdata'
blob_name = 'sales_data.csv'  # Make sure this is exactly the file name you uploaded

# Paste your access key here as a string, inside quotes
account_key = 'Azure_access_key'

# Create the BlobServiceClient
connection_string = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get the blob client
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Download blob content
blob_data = blob_client.download_blob().readall()

# Convert to DataFrame
csv_data = blob_data.decode('utf-8')
df = pd.read_csv(StringIO(csv_data))

# Show data
df.head()


# In[74]:


import sqlite3

conn = sqlite3.connect(":memory:")

df.to_sql("sales",conn,index = False,if_exists='replace')


# In[77]:


pd.read_sql("SELECT SUM(sale_amount) AS total_revenue FROM sales",conn)


# In[76]:


df.columns


# In[78]:


pd.read_sql("""select product_name,SUM(sale_amount) AS total_sales
               FROM sales
               GROUP BY product_name
               ORDER BY total_sales DESC
               LIMIT 5
               """,conn)


# In[85]:


monthly_df = pd.read_sql(""" SELECT strftime('%Y-%m', sale_date) AS month,sum(sale_amount) AS monthly_revenue
                FROM sales
                GROUP BY month
                ORDER BY month
                """,conn)

monthly_df.set_index('month',inplace = True)
monthly_df


# In[86]:


pd.read_sql("""
    SELECT customer_name, SUM(sale_amount) AS total_spent
    FROM sales
    GROUP BY customer_name
    ORDER BY total_spent DESC
    LIMIT 10
""", conn)


# In[89]:


pd.read_sql("""
    SELECT product_name, SUM(quantity) AS total_units_sold
    FROM sales
    GROUP BY product_name
    ORDER BY total_units_sold DESC
    LIMIT 10
""", conn)


# In[90]:


pd.read_sql("""
    SELECT region, category, SUM(sale_amount) AS total_sales
    FROM sales
    GROUP BY region, category
    ORDER BY region, total_sales DESC
""", conn)


# In[91]:


pd.read_sql("""
    WITH monthly_sales AS (
        SELECT strftime('%Y-%m', sale_date) AS month, 
               SUM(sale_amount) AS revenue
        FROM sales
        GROUP BY month
    )
    SELECT month,
           revenue,
           ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 / 
           LAG(revenue) OVER (ORDER BY month), 2) AS growth_percentage
    FROM monthly_sales
""", conn)


# In[92]:


pd.read_sql("""
    SELECT s1.month, s1.category, s1.total_sales
    FROM (
        SELECT strftime('%Y-%m', sale_date) AS month, 
               category, 
               SUM(sale_amount) AS total_sales
        FROM sales
        GROUP BY month, category
    ) s1
    WHERE NOT EXISTS (
        SELECT 1 FROM (
            SELECT strftime('%Y-%m', sale_date) AS month, 
                   category, 
                   SUM(sale_amount) AS total_sales
            FROM sales
            GROUP BY month, category
        ) s2
        WHERE s1.month = s2.month AND s2.total_sales > s1.total_sales
    )
""", conn)


# In[102]:


#Top 10 Customers by Revenue 


plt.figure(figsize=(10,6))
sns.barplot(
    data=top_customers,
    x='total_spent',
    y='customer_name',
    color='skyblue'  # or pick any color like 'steelblue', 'salmon', etc.
)
plt.title("Top 10 Customers by Revenue")
plt.xlabel("Total Revenue")
plt.ylabel("Customer")
plt.tight_layout()
plt.savefig("Top 10 Customers by Revenue")
plt.show()


# In[103]:


#Most Popular Products by Quantity Sold


top_products = df.groupby('product_name')['quantity'].sum().reset_index(name='total_units_sold')
top_products = top_products.sort_values(by='total_units_sold', ascending=False).head(10)


plt.figure(figsize=(10,6))
sns.barplot(
    data=top_products,
    x='total_units_sold',
    y='product_name',
    hue='product_name',
    dodge=False,
    legend=False,
    palette='magma'
)
plt.title("Top 10 Products by Quantity Sold")
plt.xlabel("Units Sold")
plt.ylabel("Product")
plt.tight_layout()
plt.savefig("Most Popular Products by Quantity Sold")
plt.show()


# In[104]:


#Sales by Region and Category (Heatmap)

# Create a pivot table for total sales by Region and Category
pivot_rc = df.pivot_table(
    values='sale_amount',
    index='region',
    columns='category',
    aggfunc='sum'
)

# Plot heatmap
plt.figure(figsize=(10, 6))
sns.heatmap(pivot_rc, annot=True, fmt=".0f", cmap='YlGnBu')
plt.title("Sales by Region and Category")
plt.xlabel("Category")
plt.ylabel("Region")
plt.tight_layout()
plt.savefig("Sales by Region and Category")
plt.show()

