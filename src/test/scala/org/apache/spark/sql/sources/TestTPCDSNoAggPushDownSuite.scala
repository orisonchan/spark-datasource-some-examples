package org.apache.spark.sql.sources

import org.apache.spark.sql.test.SharedSQLContext

class TestTPCDSNoAggPushDownSuite extends DataSourceTest with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    sql(
      """
        create table date_dim
        |(
        |    d_date_sk                 integer                       ,
        |    d_date_id                 char(16)                      ,
        |    d_date                    date                          ,
        |    d_month_seq               integer                       ,
        |    d_week_seq                integer                       ,
        |    d_quarter_seq             integer                       ,
        |    d_year                    integer                       ,
        |    d_dow                     integer                       ,
        |    d_moy                     integer                       ,
        |    d_dom                     integer                       ,
        |    d_qoy                     integer                       ,
        |    d_fy_year                 integer                       ,
        |    d_fy_quarter_seq          integer                       ,
        |    d_fy_week_seq             integer                       ,
        |    d_day_name                char(9)                       ,
        |    d_quarter_name            char(6)                       ,
        |    d_holiday                 char(1)                       ,
        |    d_weekend                 char(1)                       ,
        |    d_following_holiday       char(1)                       ,
        |    d_first_dom               integer                       ,
        |    d_last_dom                integer                       ,
        |    d_same_day_ly             integer                       ,
        |    d_same_day_lq             integer                       ,
        |    d_current_day             char(1)                       ,
        |    d_current_week            char(1)                       ,
        |    d_current_month           char(1)                       ,
        |    d_current_quarter         char(1)                       ,
        |    d_current_year            char(1)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'date_dim')
      """.stripMargin)
    sql(
      """
        create table time_dim
        |(
        |    t_time_sk                 integer                       ,
        |    t_time_id                 char(16)                      ,
        |    t_time                    integer                       ,
        |    t_hour                    integer                       ,
        |    t_minute                  integer                       ,
        |    t_second                  integer                       ,
        |    t_am_pm                   char(2)                       ,
        |    t_shift                   char(20)                      ,
        |    t_sub_shift               char(20)                      ,
        |    t_meal_time               char(20)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'time_dim')
      """.stripMargin)
    sql(
      """
        create table customer_address
        |(
        |    ca_address_sk             integer                       ,
        |    ca_address_id             char(16)                      ,
        |    ca_street_number          char(10)                      ,
        |    ca_street_name            varchar(60)                   ,
        |    ca_street_type            char(15)                      ,
        |    ca_suite_number           char(10)                      ,
        |    ca_city                   varchar(60)                   ,
        |    ca_county                 varchar(30)                   ,
        |    ca_state                  char(2)                       ,
        |    ca_zip                    char(10)                      ,
        |    ca_country                varchar(20)                   ,
        |    ca_gmt_offset             decimal(5,2)                  ,
        |    ca_location_type          char(20)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'customer_address')
      """.stripMargin)
    sql(
      """
        create table customer_demographics
        |(
        |    cd_demo_sk                integer                       ,
        |    cd_gender                 char(1)                       ,
        |    cd_marital_status         char(1)                       ,
        |    cd_education_status       char(20)                      ,
        |    cd_purchase_estimate      integer                       ,
        |    cd_credit_rating          char(10)                      ,
        |    cd_dep_count              integer                       ,
        |    cd_dep_employed_count     integer                       ,
        |    cd_dep_college_count      integer
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'customer_demographics')
      """.stripMargin)
    sql(
      """
        create table warehouse
        |(
        |    w_warehouse_sk            integer                       ,
        |    w_warehouse_id            char(16)                      ,
        |    w_warehouse_name          varchar(20)                   ,
        |    w_warehouse_sq_ft         integer                       ,
        |    w_street_number           char(10)                      ,
        |    w_street_name             varchar(60)                   ,
        |    w_street_type             char(15)                      ,
        |    w_suite_number            char(10)                      ,
        |    w_city                    varchar(60)                   ,
        |    w_county                  varchar(30)                   ,
        |    w_state                   char(2)                       ,
        |    w_zip                     char(10)                      ,
        |    w_country                 varchar(20)                   ,
        |    w_gmt_offset              decimal(5,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'warehouse')
      """.stripMargin)
    sql(
      """
        create table ship_mode
        |(
        |    sm_ship_mode_sk           integer                       ,
        |    sm_ship_mode_id           char(16)                      ,
        |    sm_type                   char(30)                      ,
        |    sm_code                   char(10)                      ,
        |    sm_carrier                char(20)                      ,
        |    sm_contract               char(20)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'ship_mode')
      """.stripMargin)
    sql(
      """
        create table reason
        |(
        |    r_reason_sk               integer                       ,
        |    r_reason_id               char(16)                      ,
        |    r_reason_desc             char(100)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'reason')
      """.stripMargin)
    sql(
      """
        create table income_band
        |(
        |    ib_income_band_sk         integer                       ,
        |    ib_lower_bound            integer                       ,
        |    ib_upper_bound            integer
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'income_band')
      """.stripMargin)
    sql(
      """
        create table item
        |(
        |    i_item_sk                 integer                       ,
        |    i_item_id                 char(16)                      ,
        |    i_rec_start_date          date                          ,
        |    i_rec_end_date            date                          ,
        |    i_item_desc               varchar(200)                  ,
        |    i_current_price           decimal(7,2)                  ,
        |    i_wholesale_cost          decimal(7,2)                  ,
        |    i_brand_id                integer                       ,
        |    i_brand                   char(50)                      ,
        |    i_class_id                integer                       ,
        |    i_class                   char(50)                      ,
        |    i_category_id             integer                       ,
        |    i_category                char(50)                      ,
        |    i_manufact_id             integer                       ,
        |    i_manufact                char(50)                      ,
        |    i_size                    char(20)                      ,
        |    i_formulation             char(20)                      ,
        |    i_color                   char(20)                      ,
        |    i_units                   char(10)                      ,
        |    i_container               char(10)                      ,
        |    i_manager_id              integer                       ,
        |    i_product_name            char(50)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'item')
      """.stripMargin)
    sql(
      """
        create table store
        |(
        |    s_store_sk                integer                       ,
        |    s_store_id                char(16)                      ,
        |    s_rec_start_date          date                          ,
        |    s_rec_end_date            date                          ,
        |    s_closed_date_sk          integer                       ,
        |    s_store_name              varchar(50)                   ,
        |    s_number_employees        integer                       ,
        |    s_floor_space             integer                       ,
        |    s_hours                   char(20)                      ,
        |    s_manager                 varchar(40)                   ,
        |    s_market_id               integer                       ,
        |    s_geography_class         varchar(100)                  ,
        |    s_market_desc             varchar(100)                  ,
        |    s_market_manager          varchar(40)                   ,
        |    s_division_id             integer                       ,
        |    s_division_name           varchar(50)                   ,
        |    s_company_id              integer                       ,
        |    s_company_name            varchar(50)                   ,
        |    s_street_number           varchar(10)                   ,
        |    s_street_name             varchar(60)                   ,
        |    s_street_type             char(15)                      ,
        |    s_suite_number            char(10)                      ,
        |    s_city                    varchar(60)                   ,
        |    s_county                  varchar(30)                   ,
        |    s_state                   char(2)                       ,
        |    s_zip                     char(10)                      ,
        |    s_country                 varchar(20)                   ,
        |    s_gmt_offset              decimal(5,2)                  ,
        |    s_tax_precentage          decimal(5,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'store')
      """.stripMargin)
    sql(
      """
        create table call_center
        |(
        |    cc_call_center_sk         integer                       ,
        |    cc_call_center_id         char(16)                      ,
        |    cc_rec_start_date         date                          ,
        |    cc_rec_end_date           date                          ,
        |    cc_closed_date_sk         integer                       ,
        |    cc_open_date_sk           integer                       ,
        |    cc_name                   varchar(50)                   ,
        |    cc_class                  varchar(50)                   ,
        |    cc_employees              integer                       ,
        |    cc_sq_ft                  integer                       ,
        |    cc_hours                  char(20)                      ,
        |    cc_manager                varchar(40)                   ,
        |    cc_mkt_id                 integer                       ,
        |    cc_mkt_class              char(50)                      ,
        |    cc_mkt_desc               varchar(100)                  ,
        |    cc_market_manager         varchar(40)                   ,
        |    cc_division               integer                       ,
        |    cc_division_name          varchar(50)                   ,
        |    cc_company                integer                       ,
        |    cc_company_name           char(50)                      ,
        |    cc_street_number          char(10)                      ,
        |    cc_street_name            varchar(60)                   ,
        |    cc_street_type            char(15)                      ,
        |    cc_suite_number           char(10)                      ,
        |    cc_city                   varchar(60)                   ,
        |    cc_county                 varchar(30)                   ,
        |    cc_state                  char(2)                       ,
        |    cc_zip                    char(10)                      ,
        |    cc_country                varchar(20)                   ,
        |    cc_gmt_offset             decimal(5,2)                  ,
        |    cc_tax_percentage         decimal(5,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'call_center')
      """.stripMargin)
    sql(
      """
        create table customer
        |(
        |    c_customer_sk             integer                       ,
        |    c_customer_id             char(16)                      ,
        |    c_current_cdemo_sk        integer                       ,
        |    c_current_hdemo_sk        integer                       ,
        |    c_current_addr_sk         integer                       ,
        |    c_first_shipto_date_sk    integer                       ,
        |    c_first_sales_date_sk     integer                       ,
        |    c_salutation              char(10)                      ,
        |    c_first_name              char(20)                      ,
        |    c_last_name               char(30)                      ,
        |    c_preferred_cust_flag     char(1)                       ,
        |    c_birth_day               integer                       ,
        |    c_birth_month             integer                       ,
        |    c_birth_year              integer                       ,
        |    c_birth_country           varchar(20)                   ,
        |    c_login                   char(13)                      ,
        |    c_email_address           char(50)                      ,
        |    c_last_review_date        char(10)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'customer')
      """.stripMargin)
    sql(
      """
        create table web_site
        |(
        |    web_site_sk               integer                       ,
        |    web_site_id               char(16)                      ,
        |    web_rec_start_date        date                          ,
        |    web_rec_end_date          date                          ,
        |    web_name                  varchar(50)                   ,
        |    web_open_date_sk          integer                       ,
        |    web_close_date_sk         integer                       ,
        |    web_class                 varchar(50)                   ,
        |    web_manager               varchar(40)                   ,
        |    web_mkt_id                integer                       ,
        |    web_mkt_class             varchar(50)                   ,
        |    web_mkt_desc              varchar(100)                  ,
        |    web_market_manager        varchar(40)                   ,
        |    web_company_id            integer                       ,
        |    web_company_name          char(50)                      ,
        |    web_street_number         char(10)                      ,
        |    web_street_name           varchar(60)                   ,
        |    web_street_type           char(15)                      ,
        |    web_suite_number          char(10)                      ,
        |    web_city                  varchar(60)                   ,
        |    web_county                varchar(30)                   ,
        |    web_state                 char(2)                       ,
        |    web_zip                   char(10)                      ,
        |    web_country               varchar(20)                   ,
        |    web_gmt_offset            decimal(5,2)                  ,
        |    web_tax_percentage        decimal(5,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'web_site')
      """.stripMargin)
    sql(
      """
        create table store_returns
        |(
        |    sr_returned_date_sk       integer                       ,
        |    sr_return_time_sk         integer                       ,
        |    sr_item_sk                integer                       ,
        |    sr_customer_sk            integer                       ,
        |    sr_cdemo_sk               integer                       ,
        |    sr_hdemo_sk               integer                       ,
        |    sr_addr_sk                integer                       ,
        |    sr_store_sk               integer                       ,
        |    sr_reason_sk              integer                       ,
        |    sr_ticket_number          integer                       ,
        |    sr_return_quantity        integer                       ,
        |    sr_return_amt             decimal(7,2)                  ,
        |    sr_return_tax             decimal(7,2)                  ,
        |    sr_return_amt_inc_tax     decimal(7,2)                  ,
        |    sr_fee                    decimal(7,2)                  ,
        |    sr_return_ship_cost       decimal(7,2)                  ,
        |    sr_refunded_cash          decimal(7,2)                  ,
        |    sr_reversed_charge        decimal(7,2)                  ,
        |    sr_store_credit           decimal(7,2)                  ,
        |    sr_net_loss               decimal(7,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'store_returns')
      """.stripMargin)
    sql(
      """
        create table household_demographics
        |(
        |    hd_demo_sk                integer                       ,
        |    hd_income_band_sk         integer                       ,
        |    hd_buy_potential          char(15)                      ,
        |    hd_dep_count              integer                       ,
        |    hd_vehicle_count          integer
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'household_demographics')
      """.stripMargin)
    sql(
      """
        create table web_page
        |(
        |    wp_web_page_sk            integer                       ,
        |    wp_web_page_id            char(16)                      ,
        |    wp_rec_start_date         date                          ,
        |    wp_rec_end_date           date                          ,
        |    wp_creation_date_sk       integer                       ,
        |    wp_access_date_sk         integer                       ,
        |    wp_autogen_flag           char(1)                       ,
        |    wp_customer_sk            integer                       ,
        |    wp_url                    varchar(100)                  ,
        |    wp_type                   char(50)                      ,
        |    wp_char_count             integer                       ,
        |    wp_link_count             integer                       ,
        |    wp_image_count            integer                       ,
        |    wp_max_ad_count           integer
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'web_page')
      """.stripMargin)
    sql(
      """
        create table promotion
        |(
        |    p_promo_sk                integer                       ,
        |    p_promo_id                char(16)                      ,
        |    p_start_date_sk           integer                       ,
        |    p_end_date_sk             integer                       ,
        |    p_item_sk                 integer                       ,
        |    p_cost                    decimal(15,2)                 ,
        |    p_response_target         integer                       ,
        |    p_promo_name              char(50)                      ,
        |    p_channel_dmail           char(1)                       ,
        |    p_channel_email           char(1)                       ,
        |    p_channel_catalog         char(1)                       ,
        |    p_channel_tv              char(1)                       ,
        |    p_channel_radio           char(1)                       ,
        |    p_channel_press           char(1)                       ,
        |    p_channel_event           char(1)                       ,
        |    p_channel_demo            char(1)                       ,
        |    p_channel_details         varchar(100)                  ,
        |    p_purpose                 char(15)                      ,
        |    p_discount_active         char(1)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'promotion')
      """.stripMargin)
    sql(
      """
        create table catalog_page
        |(
        |    cp_catalog_page_sk        integer                       ,
        |    cp_catalog_page_id        char(16)                      ,
        |    cp_start_date_sk          integer                       ,
        |    cp_end_date_sk            integer                       ,
        |    cp_department             varchar(50)                   ,
        |    cp_catalog_number         integer                       ,
        |    cp_catalog_page_number    integer                       ,
        |    cp_description            varchar(100)                  ,
        |    cp_type                   varchar(100)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'catalog_page')
      """.stripMargin)
    sql(
      """
        create table inventory
        |(
        |    inv_date_sk               integer                       ,
        |    inv_item_sk               integer                       ,
        |    inv_warehouse_sk          integer                       ,
        |    inv_quantity_on_hand      integer
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'inventory')
      """.stripMargin)
    sql(
      """
        create table catalog_returns
        |(
        |    cr_returned_date_sk       integer                       ,
        |    cr_returned_time_sk       integer                       ,
        |    cr_item_sk                integer                       ,
        |    cr_refunded_customer_sk   integer                       ,
        |    cr_refunded_cdemo_sk      integer                       ,
        |    cr_refunded_hdemo_sk      integer                       ,
        |    cr_refunded_addr_sk       integer                       ,
        |    cr_returning_customer_sk  integer                       ,
        |    cr_returning_cdemo_sk     integer                       ,
        |    cr_returning_hdemo_sk     integer                       ,
        |    cr_returning_addr_sk      integer                       ,
        |    cr_call_center_sk         integer                       ,
        |    cr_catalog_page_sk        integer                       ,
        |    cr_ship_mode_sk           integer                       ,
        |    cr_warehouse_sk           integer                       ,
        |    cr_reason_sk              integer                       ,
        |    cr_order_number           integer                       ,
        |    cr_return_quantity        integer                       ,
        |    cr_return_amount          decimal(7,2)                  ,
        |    cr_return_tax             decimal(7,2)                  ,
        |    cr_return_amt_inc_tax     decimal(7,2)                  ,
        |    cr_fee                    decimal(7,2)                  ,
        |    cr_return_ship_cost       decimal(7,2)                  ,
        |    cr_refunded_cash          decimal(7,2)                  ,
        |    cr_reversed_charge        decimal(7,2)                  ,
        |    cr_store_credit           decimal(7,2)                  ,
        |    cr_net_loss               decimal(7,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'catalog_returns')
      """.stripMargin)
    sql(
      """
        create table web_returns
        |(
        |    wr_returned_date_sk       integer                       ,
        |    wr_returned_time_sk       integer                       ,
        |    wr_item_sk                integer                       ,
        |    wr_refunded_customer_sk   integer                       ,
        |    wr_refunded_cdemo_sk      integer                       ,
        |    wr_refunded_hdemo_sk      integer                       ,
        |    wr_refunded_addr_sk       integer                       ,
        |    wr_returning_customer_sk  integer                       ,
        |    wr_returning_cdemo_sk     integer                       ,
        |    wr_returning_hdemo_sk     integer                       ,
        |    wr_returning_addr_sk      integer                       ,
        |    wr_web_page_sk            integer                       ,
        |    wr_reason_sk              integer                       ,
        |    wr_order_number           integer                       ,
        |    wr_return_quantity        integer                       ,
        |    wr_return_amt             decimal(7,2)                  ,
        |    wr_return_tax             decimal(7,2)                  ,
        |    wr_return_amt_inc_tax     decimal(7,2)                  ,
        |    wr_fee                    decimal(7,2)                  ,
        |    wr_return_ship_cost       decimal(7,2)                  ,
        |    wr_refunded_cash          decimal(7,2)                  ,
        |    wr_reversed_charge        decimal(7,2)                  ,
        |    wr_account_credit         decimal(7,2)                  ,
        |    wr_net_loss               decimal(7,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'web_returns')
      """.stripMargin)
    sql(
      """
        create table web_sales
        |(
        |    ws_sold_date_sk           integer                       ,
        |    ws_sold_time_sk           integer                       ,
        |    ws_ship_date_sk           integer                       ,
        |    ws_item_sk                integer                       ,
        |    ws_bill_customer_sk       integer                       ,
        |    ws_bill_cdemo_sk          integer                       ,
        |    ws_bill_hdemo_sk          integer                       ,
        |    ws_bill_addr_sk           integer                       ,
        |    ws_ship_customer_sk       integer                       ,
        |    ws_ship_cdemo_sk          integer                       ,
        |    ws_ship_hdemo_sk          integer                       ,
        |    ws_ship_addr_sk           integer                       ,
        |    ws_web_page_sk            integer                       ,
        |    ws_web_site_sk            integer                       ,
        |    ws_ship_mode_sk           integer                       ,
        |    ws_warehouse_sk           integer                       ,
        |    ws_promo_sk               integer                       ,
        |    ws_order_number           integer                       ,
        |    ws_quantity               integer                       ,
        |    ws_wholesale_cost         decimal(7,2)                  ,
        |    ws_list_price             decimal(7,2)                  ,
        |    ws_sales_price            decimal(7,2)                  ,
        |    ws_ext_discount_amt       decimal(7,2)                  ,
        |    ws_ext_sales_price        decimal(7,2)                  ,
        |    ws_ext_wholesale_cost     decimal(7,2)                  ,
        |    ws_ext_list_price         decimal(7,2)                  ,
        |    ws_ext_tax                decimal(7,2)                  ,
        |    ws_coupon_amt             decimal(7,2)                  ,
        |    ws_ext_ship_cost          decimal(7,2)                  ,
        |    ws_net_paid               decimal(7,2)                  ,
        |    ws_net_paid_inc_tax       decimal(7,2)                  ,
        |    ws_net_paid_inc_ship      decimal(7,2)                  ,
        |    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
        |    ws_net_profit             decimal(7,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'web_sales')
      """.stripMargin)
    sql(
      """
        create table catalog_sales
        |(
        |    cs_sold_date_sk           integer                       ,
        |    cs_sold_time_sk           integer                       ,
        |    cs_ship_date_sk           integer                       ,
        |    cs_bill_customer_sk       integer                       ,
        |    cs_bill_cdemo_sk          integer                       ,
        |    cs_bill_hdemo_sk          integer                       ,
        |    cs_bill_addr_sk           integer                       ,
        |    cs_ship_customer_sk       integer                       ,
        |    cs_ship_cdemo_sk          integer                       ,
        |    cs_ship_hdemo_sk          integer                       ,
        |    cs_ship_addr_sk           integer                       ,
        |    cs_call_center_sk         integer                       ,
        |    cs_catalog_page_sk        integer                       ,
        |    cs_ship_mode_sk           integer                       ,
        |    cs_warehouse_sk           integer                       ,
        |    cs_item_sk                integer                       ,
        |    cs_promo_sk               integer                       ,
        |    cs_order_number           integer                       ,
        |    cs_quantity               integer                       ,
        |    cs_wholesale_cost         decimal(7,2)                  ,
        |    cs_list_price             decimal(7,2)                  ,
        |    cs_sales_price            decimal(7,2)                  ,
        |    cs_ext_discount_amt       decimal(7,2)                  ,
        |    cs_ext_sales_price        decimal(7,2)                  ,
        |    cs_ext_wholesale_cost     decimal(7,2)                  ,
        |    cs_ext_list_price         decimal(7,2)                  ,
        |    cs_ext_tax                decimal(7,2)                  ,
        |    cs_coupon_amt             decimal(7,2)                  ,
        |    cs_ext_ship_cost          decimal(7,2)                  ,
        |    cs_net_paid               decimal(7,2)                  ,
        |    cs_net_paid_inc_tax       decimal(7,2)                  ,
        |    cs_net_paid_inc_ship      decimal(7,2)                  ,
        |    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
        |    cs_net_profit             decimal(7,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'catalog_sales')
      """.stripMargin)
    sql(
      """
        create table store_sales
        |(
        |    ss_sold_date_sk           integer                       ,
        |    ss_sold_time_sk           integer                       ,
        |    ss_item_sk                integer                       ,
        |    ss_customer_sk            integer                       ,
        |    ss_cdemo_sk               integer                       ,
        |    ss_hdemo_sk               integer                       ,
        |    ss_addr_sk                integer                       ,
        |    ss_store_sk               integer                       ,
        |    ss_promo_sk               integer                       ,
        |    ss_ticket_number          integer                       ,
        |    ss_quantity               integer                       ,
        |    ss_wholesale_cost         decimal(7,2)                  ,
        |    ss_list_price             decimal(7,2)                  ,
        |    ss_sales_price            decimal(7,2)                  ,
        |    ss_ext_discount_amt       decimal(7,2)                  ,
        |    ss_ext_sales_price        decimal(7,2)                  ,
        |    ss_ext_wholesale_cost     decimal(7,2)                  ,
        |    ss_ext_list_price         decimal(7,2)                  ,
        |    ss_ext_tax                decimal(7,2)                  ,
        |    ss_coupon_amt             decimal(7,2)                  ,
        |    ss_net_paid               decimal(7,2)                  ,
        |    ss_net_paid_inc_tax       decimal(7,2)                  ,
        |    ss_net_profit             decimal(7,2)
        |)
        |USING org.apache.spark.sql.sources.examples.TPCDSDataSource
        |OPTIONS ("table" 'store_sales')
      """.stripMargin)
  }

  //    test("select * from store") {
  //      val df = spark.sql("select * from store limit 10")
  //      df.show(false)
  //    }

  //  test("select * from time_dim") {
  //    val df = spark.sql("select * from time_dim limit 10")
  //    df.show(false)
  //  }
  //
  //  test("select * from date_dim") {
  //    val df = spark.sql("select * from date_dim limit 10")
  //    df.show(false)
  //  }
  //
  //  test("select t_time_sk, t_time, t_hour, t_minute, t_second, t_am_pm from time_dim") {
  //    val df = spark.sql("select t_time_sk, t_am_pm from time_dim limit 10")
  //    df.show(false)
  //  }
  //
  //  test("select d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year from date_dim ") {
  //    val df = spark.sql("select d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year from date_dim limit 10")
  //    df.show(false)
  //  }
  //
  //  test("select * from time_dim where") {
  //    val df = spark.sql("select * from time_dim where t_time_sk < 100")
  //    df.show(false)
  //  }
  //
  //    test("select * from time_dim where in ") {
  //      val df = spark.sql("select * from time_dim where t_time_sk in (1,2,3,4,5,100)")
  //      df.show(false)
  //    }

  test("sql 1") {
    val df =
      spark.sql(
        """
          select * from (select  count(*)
          |from store_sales
          |    ,household_demographics
          |    ,time_dim, store
          |where ss_sold_time_sk = time_dim.t_time_sk
          |    and ss_hdemo_sk = household_demographics.hd_demo_sk
          |    and ss_store_sk = s_store_sk
          |    and time_dim.t_hour = 8
          |    and time_dim.t_minute >= 30
          |    and household_demographics.hd_dep_count = 5
          |    and store.s_store_name = 'ese'
          |order by count(*)
          | ) limit 100
        """.stripMargin)
    df.show()
  }

  //    test("sql 2") {
  //      val df =
  //        spark.sql(
  //          """
  //          select * from (select  i_item_id,
  //          |        avg(ss_quantity) agg1,
  //          |        avg(ss_list_price) agg2,
  //          |        avg(ss_coupon_amt) agg3,
  //          |        avg(ss_sales_price) agg4
  //          | from store_sales, customer_demographics, date_dim, item, promotion
  //          | where ss_sold_date_sk = d_date_sk and
  //          |       ss_item_sk = i_item_sk and
  //          |       ss_cdemo_sk = cd_demo_sk and
  //          |       ss_promo_sk = p_promo_sk and
  //          |       cd_gender = 'M' and
  //          |       cd_marital_status = 'M' and
  //          |       cd_education_status = '4 yr Degree' and
  //          |       (p_channel_email = 'N' or p_channel_event = 'N') and
  //          |       d_year = 2001
  //          | group by i_item_id
  //          | order by i_item_id
  //          |  ) limit 100
  //        """.stripMargin)
  //      df.show(false)
  //    }

  //    test("sql 4"){
  //      val df =
  //        spark.sql(
  //          """
  //            select * from (select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
  //            |from(select *
  //            |     from (select item_sk,rank() over (order by rank_col asc) rnk
  //            |           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
  //            |                 from store_sales ss1
  //            |                 where ss_store_sk = 6
  //            |                 group by ss_item_sk
  //            |                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
  //            |                                                  from store_sales
  //            |                                                  where ss_store_sk = 6
  //            |                                                    and ss_hdemo_sk is null
  //            |                                                  group by ss_store_sk))V1)V11
  //            |     where rnk  < 11) asceding,
  //            |    (select *
  //            |     from (select item_sk,rank() over (order by rank_col desc) rnk
  //            |           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
  //            |                 from store_sales ss1
  //            |                 where ss_store_sk = 6
  //            |                 group by ss_item_sk
  //            |                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
  //            |                                                  from store_sales
  //            |                                                  where ss_store_sk = 6
  //            |                                                    and ss_hdemo_sk is null
  //            |                                                  group by ss_store_sk))V2)V21
  //            |     where rnk  < 11) descending,
  //            |item i1,
  //            |item i2
  //            |where asceding.rnk = descending.rnk
  //            |  and i1.i_item_sk=asceding.item_sk
  //            |  and i2.i_item_sk=descending.item_sk
  //            |order by asceding.rnk
  //            | ) limit 100
  //          """.stripMargin
  //        )
  //      df.show(false)
  //    }

  test("sql 7") {
    val df =
      spark.sql(
        """
            select *
            |from
            |(
            |  select sum(cs_ext_discount_amt)
            |    from
            |      catalog_sales
            |     ,item
            |     ,date_dim
            |  where
            |    i_manufact_id = 283
            |    and i_item_sk = cs_item_sk
            |    and d_date between '1999-02-22' and cast('1999-05-22' as date)
            |    and d_date_sk = cs_sold_date_sk
            |    and cs_ext_discount_amt
            |     > (
            |         select
            |            1.3 * avg(cs_ext_discount_amt)
            |         from
            |            catalog_sales
            |           ,date_dim
            |         where
            |              cs_item_sk = i_item_sk
            |          and d_date between '1999-02-22' and
            |                             cast('1999-05-22' as date)
            |          and d_date_sk = cs_sold_date_sk
            |      )
            | ) limit 100
          """.stripMargin
      )
    df.show(false)
  }

  test("sql 8") {
    val df = spark.sql(
      """
        |select * from (select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
        | 	sum(ss_ext_sales_price) ext_price
        | from date_dim, store_sales, item,customer,customer_address,store
        | where d_date_sk = ss_sold_date_sk
        |   and ss_item_sk = i_item_sk
        |   and d_moy=11
        |   and ss_customer_sk = c_customer_sk
        |   and c_current_addr_sk = ca_address_sk
        |   and ss_store_sk = s_store_sk
        | group by i_brand
        |      ,i_brand_id
        |      ,i_manufact_id
        |      ,i_manufact
        | order by ext_price desc
        |         ,i_brand
        |         ,i_brand_id
        |         ,i_manufact_id
        |         ,i_manufact
        | ) limit 100
      """.stripMargin)
    df.show(false)
  }

  //  test("sql 9") {
  //    val df = spark.sql(
  //      """
  //        |select * from (select
  //        | i_item_id
  //        | ,i_item_desc
  //        | ,s_store_id
  //        | ,s_store_name
  //        | ,min(ss_net_profit) as store_sales_profit
  //        | ,min(sr_net_loss) as store_returns_loss
  //        | ,min(cs_net_profit) as catalog_sales_profit
  //        | from
  //        | store_sales
  //        | ,store_returns
  //        | ,catalog_sales
  //        | ,date_dim d1
  //        | ,date_dim d2
  //        | ,date_dim d3
  //        | ,store
  //        | ,item
  //        | where
  //        | d1.d_moy = 4
  //        | and d1.d_year = 2002
  //        | and d1.d_date_sk = ss_sold_date_sk
  //        | and i_item_sk = ss_item_sk
  //        | and s_store_sk = ss_store_sk
  //        | and ss_customer_sk = sr_customer_sk
  //        | and ss_item_sk = sr_item_sk
  //        | and ss_ticket_number = sr_ticket_number
  //        | and sr_returned_date_sk = d2.d_date_sk
  //        | and d2.d_moy               between 4 and  10
  //        | and d2.d_year              = 2002
  //        | and sr_customer_sk = cs_bill_customer_sk
  //        | and sr_item_sk = cs_item_sk
  //        | and cs_sold_date_sk = d3.d_date_sk
  //        | and d3.d_moy               between 4 and  10
  //        | and d3.d_year              = 2002
  //        | group by
  //        | i_item_id
  //        | ,i_item_desc
  //        | ,s_store_id
  //        | ,s_store_name
  //        | order by
  //        | i_item_id
  //        | ,i_item_desc
  //        | ,s_store_id
  //        | ,s_store_name
  //        |  ) limit 100
  //      """.stripMargin
  //    )
  //    df.show(false)
  //  }


}
