# Data Analytics Questionnaire
Questions for Senior Analytics Engineering position (with answers)

### 1. Please refactor this SQL snippet so it's performant on an OLAP database, like Snowflake.

#### Initial snippet:
```sql
select
    customer_id,
    first_name,
    last_name,
    (select round(avg(total_amount),2) from {{ ref('orders') }} where orders.customer_id = customers.customer_id and ordered_at > current_date - 180) as avg_order_amount,
    (select count(*) from {{ ref('orders') }} where orders.customer_id = customers.customer_id and ordered_at > current_date - 180) as order_count
from {{ ref('customers') }}
where customer_id in (
  select distinct customer_id
  from {{ ref('orders') }}
  where ordered_at > current_date - 42
)
```

#### Answer:

```sql
-- Increase query performance by replacing subqueries with CTEs

-- Calculate average order amount and order count by customer on orders for the last 180 days
with act_orders as (
    select 
        customer_id
        , round(avg(total_amount),2) as avg_order_amount
        , count(*) as order_count
    from {{ ref('orders') }} 
    where 1=1
        and ordered_at > current_date - 180
    group by customer_id
),
-- Calculate unique customers that made order within last 42 days
act_customers as (
    select distinct customer_id
    from {{ ref('orders') }}
    where 1=1
        and ordered_at > current_date - 42
)
select
    c.customer_id
    , c.first_name
    , c.last_name
    , ao.avg_order_amount
    , ao.order_count
from {{ ref('customers') }} c 
inner join act_customers ac on c.customer_id = ac.customer_id
left join act_orders ao on c.customer_id = ao.customer_id
```

### 2. Suppose we have the following schema with two tables: Customers and usage.

Please write a SQL query to calculate the rolling average credit consumption over the last seven days, and highlight the top 5 credit consuming customers.
* Customers (customer_id, account_locator, customer name, status)
    * status could be active or inactive
* Usage (event_id, customer_id, credits_consumed, date, price per credit)

```sql
-- Calculate total amount of credits consumed by active customers
with act_customers as (
    select 
        u.customer_id
        , u.date
        , sum(u.credits_consumed * u.price_per_credit) as total_consumed
    from usage u 
    inner join customers c on u.customer_id = c.customer_id
    where 1=1
        and c.status = 'active'
    group by u.customer_id, u.date
)
-- Calculate rolling average amount of consumed credits by customers
, rolling_avg as (
    select
        customer_id
        , avg(total_consumed) over(partition by customer_id order by date range between '7 day' preceding and current row ) as avg_consumption
    from act_customers
    )
-- Rank customers by rolling average amount of consumed credits
, ranked_customers as (
    select
        customer_id
        , avg_consumption
        , rank() over(order by avg_consumption desc) as rank
    from rolling_avg
)
-- Top-5 customers by rolling average amount of consumed credits 
select
    customer_id
    , avg_consumption
from ranked_customers
where 1=1
    and rank <= 5
```

### 3. What is the dbt command to build the ‘usage’ model including all upstream and downstream models (include related tests as well)

dbt build --select +usage+

'dbt build' command runs and tests model
'--select' command choose spefic model
'+' before model name includes all upstream models
'+' after model name includes all downstream models

### 4. Assuming the ‘usage’ model lives in the analytics.production schema, what grants would the role assigned to the dbt user require in order to build the model? 

In order to build the ‘usage’ model in the ‘analytics.production’ schema in Snowflake, the role assigned to the dbt user would require the following grants:

The following roles should be granted to dbt user in the 'analytics' database:

* USAGE on all schemas,SELECT on all tables, SELECT on all views, CREATE on the ‘analytics’ database.

You can grant these permissions to the role assigned to the dbt user by running the following SQL statements:

```sql
GRANT USAGE ON DATABASE analytics TO ROLE dbt_role;
GRANT USAGE ON SCHEMA analytics.production TO ROLE dbt_role;
GRANT CREATE TABLE ON SCHEMA analytics.production TO ROLE dbt_role;
GRANT CREATE VIEW ON SCHEMA analytics.production TO ROLE dbt_role;
```
