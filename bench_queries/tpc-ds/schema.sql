
CREATE TABLE DBGEN_VERSION
(
    dv_version                String,
    dv_create_date            Date,
    dv_create_time            String,
    dv_cmdline_args           String
);

CREATE TABLE CUSTOMER_ADDRESS
(
    ca_address_sk             BIGINT,
    ca_address_id             String,
    ca_street_number          String,
    ca_street_name            String,
    ca_street_type            String,
    ca_suite_number           String,
    ca_city                   String,
    ca_county                 String,
    ca_state                  String,
    ca_zip                    String,
    ca_country                String,
    ca_gmt_offset             Float,
    ca_location_type          String
);

CREATE TABLE CUSTOMER_DEMOGRAPHICS
(
    cd_demo_sk                BIGINT,
    cd_gender                 String,
    cd_marital_status         String,
    cd_education_status       String,
    cd_purchase_estimate      BIGINT,
    cd_credit_rating          String,
    cd_dep_count              BIGINT,
    cd_dep_employed_count     BIGINT,
    cd_dep_college_count      BIGINT
);

CREATE TABLE DATE_DIM
(
    d_date_sk                 BIGINT,
    d_date_id                 String,
    d_date                    Date,
    d_month_seq               BIGINT,
    d_week_seq                BIGINT,
    d_quarter_seq             BIGINT,
    d_year                    BIGINT,
    d_dow                     BIGINT,
    d_moy                     BIGINT,
    d_dom                     BIGINT,
    d_qoy                     BIGINT,
    d_fy_year                 BIGINT,
    d_fy_quarter_seq          BIGINT,
    d_fy_week_seq             BIGINT,
    d_day_name                String,
    d_quarter_name            String,
    d_holiday                 String,
    d_weekend                 String,
    d_following_holiday       String,
    d_first_dom               BIGINT,
    d_last_dom                BIGINT,
    d_same_day_ly             BIGINT,
    d_same_day_lq             BIGINT,
    d_current_day             String,
    d_current_week            String,
    d_current_month           String,
    d_current_quarter         String,
    d_current_year            String
);

CREATE TABLE WAREHOUSE
(
    w_warehouse_sk            BIGINT,
    w_warehouse_id            String,
    w_warehouse_name          String,
    w_warehouse_sq_ft         BIGINT,
    w_street_number           String,
    w_street_name             String,
    w_street_type             String,
    w_suite_number            String,
    w_city                    String,
    w_county                  String,
    w_state                   String,
    w_zip                     String,
    w_country                 String,
    w_gmt_offset              Float
);

CREATE TABLE SHIP_MODE
(
    sm_ship_mode_sk           BIGINT,
    sm_ship_mode_id           String,
    sm_type                   String,
    sm_code                   String,
    sm_carrier                String,
    sm_contract               String
);

CREATE TABLE TIME_DIM
(
    t_time_sk                 BIGINT,
    t_time_id                 String,
    t_time                    BIGINT,
    t_hour                    BIGINT,
    t_minute                  BIGINT,
    t_second                  BIGINT,
    t_am_pm                   String,
    t_shift                   String,
    t_sub_shift               String,
    t_meal_time               String
);

CREATE TABLE REASON
(
    r_reason_sk               BIGINT,
    r_reason_id               String,
    r_reason_desc             String
);

CREATE TABLE INCOME_BAND
(
    ib_income_band_sk         BIGINT,
    ib_lower_bound            BIGINT,
    ib_upper_bound            BIGINT
);

CREATE TABLE ITEM
(
    i_item_sk                 BIGINT,
    i_item_id                 String,
    i_rec_start_date          Date,
    i_rec_end_date            Date,
    i_item_desc               String,
    i_current_price           Float,
    i_wholesale_cost          Float,
    i_brand_id                BIGINT,
    i_brand                   String,
    i_class_id                BIGINT,
    i_class                   String,
    i_category_id             BIGINT,
    i_category                String,
    i_manufact_id             BIGINT,
    i_manufact                String,
    i_size                    String,
    i_formulation             String,
    i_color                   String,
    i_units                   String,
    i_container               String,
    i_manager_id              BIGINT,
    i_product_name            String
);

CREATE TABLE STORE
(
    s_store_sk                BIGINT,
    s_store_id                String,
    s_rec_start_date          Date,
    s_rec_end_date            Date,
    s_closed_date_sk          BIGINT,
    s_store_name              String,
    s_number_employees        BIGINT,
    s_floor_space             BIGINT,
    s_hours                   String,
    s_manager                 String,
    s_market_id               BIGINT,
    s_geography_class         String,
    s_market_desc             String,
    s_market_manager          String,
    s_division_id             BIGINT,
    s_division_name           String,
    s_company_id              BIGINT,
    s_company_name            String,
    s_street_number           String,
    s_street_name             String,
    s_street_type             String,
    s_suite_number            String,
    s_city                    String,
    s_county                  String,
    s_state                   String,
    s_zip                     String,
    s_country                 String,
    s_gmt_offset              BIGINT,
    s_tax_precentage          Float
);

CREATE TABLE CALL_CENTER
(
    cc_call_center_sk         BIGINT,
    cc_call_center_id         String,
    cc_rec_start_date         Date,
    cc_rec_end_date           Date,
    cc_closed_date_sk         BIGINT,
    cc_open_date_sk           BIGINT,
    cc_name                   String,
    cc_class                  String,
    cc_employees              BIGINT,
    cc_sq_ft                  BIGINT,
    cc_hours                  String,
    cc_manager                String,
    cc_mkt_id                 BIGINT,
    cc_mkt_class              String,
    cc_mkt_desc               String,
    cc_market_manager         String,
    cc_division               BIGINT,
    cc_division_name          String,
    cc_company                BIGINT,
    cc_company_name           String,
    cc_street_number          String,
    cc_street_name            String,
    cc_street_type            String,
    cc_suite_number           String,
    cc_city                   String,
    cc_county                 String,
    cc_state                  String,
    cc_zip                    String,
    cc_country                String,
    cc_gmt_offset             Float,
    cc_tax_percentage         Float
);

CREATE TABLE CUSTOMER
(
    c_customer_sk             BIGINT,
    c_customer_id             String,
    c_current_cdemo_sk        BIGINT,
    c_current_hdemo_sk        BIGINT,
    c_current_addr_sk         BIGINT,
    c_first_shipto_date_sk    BIGINT,
    c_first_sales_date_sk     BIGINT,
    c_salutation              String,
    c_first_name              String,
    c_last_name               String,
    c_preferred_cust_flag     String,
    c_birth_day               BIGINT,
    c_birth_month             BIGINT,
    c_birth_year              BIGINT,
    c_birth_country           String,
    c_login                   String,
    c_email_address           String,
    c_last_review_date        String
);

CREATE TABLE WEB_SITE
(
    web_site_sk               BIGINT,
    web_site_id               String,
    web_rec_start_date        Date,
    web_rec_end_date          Date,
    web_name                  String,
    web_open_date_sk          BIGINT,
    web_close_date_sk         BIGINT,
    web_class                 String,
    web_manager               String,
    web_mkt_id                BIGINT,
    web_mkt_class             String,
    web_mkt_desc              String,
    web_market_manager        String,
    web_company_id            BIGINT,
    web_company_name          String,
    web_street_number         String,
    web_street_name           String,
    web_street_type           String,
    web_suite_number          String,
    web_city                  String,
    web_county                String,
    web_state                 String,
    web_zip                   String,
    web_country               String,
    web_gmt_offset            Float,
    web_tax_percentage        Float
);

CREATE TABLE STORE_RETURNS
(
    sr_returned_date_sk       BIGINT,
    sr_return_time_sk         BIGINT,
    sr_item_sk                BIGINT,
    sr_customer_sk            BIGINT,
    sr_cdemo_sk               BIGINT,
    sr_hdemo_sk               BIGINT,
    sr_addr_sk                BIGINT,
    sr_store_sk               BIGINT,
    sr_reason_sk              BIGINT,
    sr_ticket_number          BIGINT,
    sr_return_quantity        BIGINT,
    sr_return_amt             Float,
    sr_return_tax             Float,
    sr_return_amt_inc_tax     Float,
    sr_fee                    Float,
    sr_return_ship_cost       Float,
    sr_refunded_cash          Float,
    sr_reversed_charge        Float,
    sr_store_credit           Float,
    sr_net_loss               Float
);

CREATE TABLE HOUSEHOLD_DEMOGRAPHICS
(
    hd_demo_sk                BIGINT,
    hd_income_band_sk         BIGINT,
    hd_buy_potential          String,
    hd_dep_count              BIGINT,
    hd_vehicle_count          BIGINT
);

CREATE TABLE web_page
(
    wp_web_page_sk            BIGINT,
    wp_web_page_id            String,
    wp_rec_start_date         Date,
    wp_rec_end_date           Date,
    wp_creation_date_sk       BIGINT,
    wp_access_date_sk         BIGINT,
    wp_autogen_flag           String,
    wp_customer_sk            BIGINT,
    wp_url                    String,
    wp_type                   String,
    wp_char_count             BIGINT,
    wp_link_count             BIGINT,
    wp_image_count            BIGINT,
    wp_max_ad_count           BIGINT
);

CREATE TABLE PROMOTION
(
    p_promo_sk                BIGINT,
    p_promo_id                String,
    p_start_date_sk           BIGINT,
    p_end_date_sk             BIGINT,
    p_item_sk                 BIGINT,
    p_cost                    DOUBLE,
    p_response_target         BIGINT,
    p_promo_name              String,
    p_channel_dmail           String,
    p_channel_email           String,
    p_channel_catalog         String,
    p_channel_tv              String,
    p_channel_radio           String,
    p_channel_press           String,
    p_channel_event           String,
    p_channel_demo            String,
    p_channel_details         String,
    p_purpose                 String,
    p_discount_active         String
);

CREATE TABLE CATALOG_PAGE
(
    cp_catalog_page_sk        BIGINT,
    cp_catalog_page_id        String,
    cp_start_date_sk          BIGINT,
    cp_end_date_sk            BIGINT,
    cp_department             String,
    cp_catalog_number         BIGINT,
    cp_catalog_page_number    BIGINT,
    cp_description            String,
    cp_type                   String
);

CREATE TABLE INVENTORY
(
    inv_date_sk               BIGINT,
    inv_item_sk               BIGINT,
    inv_warehouse_sk          BIGINT,
    inv_quantity_on_hand      BIGINT
);

CREATE TABLE CATALOG_RETURNS
(
    cr_returned_date_sk       BIGINT,
    cr_returned_time_sk       BIGINT,
    cr_item_sk                BIGINT,
    cr_refunded_customer_sk   BIGINT,
    cr_refunded_cdemo_sk      BIGINT,
    cr_refunded_hdemo_sk      BIGINT,
    cr_refunded_addr_sk       BIGINT,
    cr_returning_customer_sk  BIGINT,
    cr_returning_cdemo_sk     BIGINT,
    cr_returning_hdemo_sk     BIGINT,
    cr_returning_addr_sk      BIGINT,
    cr_call_center_sk         BIGINT,
    cr_catalog_page_sk        BIGINT,
    cr_ship_mode_sk           BIGINT,
    cr_warehouse_sk           BIGINT,
    cr_reason_sk              BIGINT,
    cr_order_number           BIGINT,
    cr_return_quantity        BIGINT,
    cr_return_amount          FLOAT,
    cr_return_tax             FLOAT,
    cr_return_amt_inc_tax     FLOAT,
    cr_fee                    FLOAT,
    cr_return_ship_cost       FLOAT,
    cr_refunded_cash          FLOAT,
    cr_reversed_charge        FLOAT,
    cr_store_credit           FLOAT,
    cr_net_loss               FLOAT
);

CREATE TABLE WEB_RETURNS
(
    wr_returned_date_sk       BIGINT,
    wr_returned_time_sk       BIGINT,
    wr_item_sk                BIGINT,
    wr_refunded_customer_sk   BIGINT,
    wr_refunded_cdemo_sk      BIGINT,
    wr_refunded_hdemo_sk      BIGINT,
    wr_refunded_addr_sk       BIGINT,
    wr_returning_customer_sk  BIGINT,
    wr_returning_cdemo_sk     BIGINT,
    wr_returning_hdemo_sk     BIGINT,
    wr_returning_addr_sk      BIGINT,
    wr_web_page_sk            BIGINT,
    wr_reason_sk              BIGINT,
    wr_order_number           BIGINT NOT NULL,
    wr_return_quantity        BIGINT,
    wr_return_amt             Float,
    wr_return_tax             Float,
    wr_return_amt_inc_tax     Float,
    wr_fee                    Float,
    wr_return_ship_cost       Float,
    wr_refunded_cash          Float,
    wr_reversed_charge        Float,
    wr_account_credit         Float,
    wr_net_loss               Float
);

CREATE TABLE WEB_SALES
(
    ws_sold_date_sk           BIGINT,
    ws_sold_time_sk           BIGINT,
    ws_ship_date_sk           BIGINT,
    ws_item_sk                BIGINT NOT NULL,
    ws_bill_customer_sk       BIGINT,
    ws_bill_cdemo_sk          BIGINT,
    ws_bill_hdemo_sk          BIGINT,
    ws_bill_addr_sk           BIGINT,
    ws_ship_customer_sk       BIGINT,
    ws_ship_cdemo_sk          BIGINT,
    ws_ship_hdemo_sk          BIGINT,
    ws_ship_addr_sk           BIGINT,
    ws_web_page_sk            BIGINT,
    ws_web_site_sk            BIGINT,
    ws_ship_mode_sk           BIGINT,
    ws_warehouse_sk           BIGINT,
    ws_promo_sk               BIGINT,
    ws_order_number           BIGINT NOT NULL,
    ws_quantity               BIGINT,
    ws_wholesale_cost         Float,
    ws_list_price             Float,
    ws_sales_price            Float,
    ws_ext_discount_amt       Float,
    ws_ext_sales_price        Float,
    ws_ext_wholesale_cost     Float,
    ws_ext_list_price         Float,
    ws_ext_tax                Float,
    ws_coupon_amt             Float,
    ws_ext_ship_cost          Float,
    ws_net_paid               Float,
    ws_net_paid_inc_tax       Float,
    ws_net_paid_inc_ship      Float,
    ws_net_paid_inc_ship_tax  Float,
    ws_net_profit             Float
);

CREATE TABLE CATALOG_SALES
(
    cs_sold_date_sk           BIGINT,
    cs_sold_time_sk           BIGINT,
    cs_ship_date_sk           BIGINT,
    cs_bill_customer_sk       BIGINT,
    cs_bill_cdemo_sk          BIGINT,
    cs_bill_hdemo_sk          BIGINT,
    cs_bill_addr_sk           BIGINT,
    cs_ship_customer_sk       BIGINT,
    cs_ship_cdemo_sk          BIGINT,
    cs_ship_hdemo_sk          BIGINT,
    cs_ship_addr_sk           BIGINT,
    cs_call_center_sk         BIGINT,
    cs_catalog_page_sk        BIGINT,
    cs_ship_mode_sk           BIGINT,
    cs_warehouse_sk           BIGINT,
    cs_item_sk                BIGINT NOT NULL,
    cs_promo_sk               BIGINT,
    cs_order_number           BIGINT NOT NULL,
    cs_quantity               BIGINT,
    cs_wholesale_cost         Float,
    cs_list_price             Float,
    cs_sales_price            Float,
    cs_ext_discount_amt       Float,
    cs_ext_sales_price        Float,
    cs_ext_wholesale_cost     Float,
    cs_ext_list_price         Float,
    cs_ext_tax                Float,
    cs_coupon_amt             Float,
    cs_ext_ship_cost          Float,
    cs_net_paid               Float,
    cs_net_paid_inc_tax       Float,
    cs_net_paid_inc_ship      Float,
    cs_net_paid_inc_ship_tax  Float,
    cs_net_profit             Float
);

CREATE TABLE STORE_SALES
(
    ss_sold_date_sk           BIGINT,
    ss_sold_time_sk           BIGINT,
    ss_item_sk                BIGINT NOT NULL,
    ss_customer_sk            BIGINT,
    ss_cdemo_sk               BIGINT,
    ss_hdemo_sk               BIGINT,
    ss_addr_sk                BIGINT,
    ss_store_sk               BIGINT,
    ss_promo_sk               BIGINT,
    ss_ticket_number          BIGINT NOT NULL,
    ss_quantity               BIGINT,
    ss_wholesale_cost         Float,
    ss_list_price             Float,
    ss_sales_price            Float,
    ss_ext_discount_amt       Float,
    ss_ext_sales_price        Float,
    ss_ext_wholesale_cost     Float,
    ss_ext_list_price         Float,
    ss_ext_tax                Float,
    ss_coupon_amt             Float,
    ss_net_paid               Float,
    ss_net_paid_inc_tax       Float,
    ss_net_profit             Float
);
