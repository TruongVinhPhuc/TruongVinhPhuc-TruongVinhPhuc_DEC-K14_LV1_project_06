-- Bảng 1: Product_Quality
CREATE TABLE Product_Quality (
  _id INT64,
  quality STRING,
  quality_label STRING
);

-- Bảng 2: Product
CREATE TABLE Product (
  _id INT64,
  product_name STRING
);

-- Bảng 3: Product_Option
CREATE TABLE Product_Option (
  _id INT64,
  option_label STRING,
  value_label STRING,
  option_id INT64,
  value_id INT64
);

-- Bảng 4: Filter_Option
CREATE TABLE Filter_Option (
  _id INT64,
  alloy STRING,
  diamond STRING,
  shapediamond STRING
);

-- Bảng 5: Recommend_Option
CREATE TABLE Recommend_Option (
  _id INT64,
  alloy STRING,
  stone STRING,
  pearlcolor STRING,
  finish STRING,
  price FLOAT64,
  kollektion STRING,
  kollektion_id INT64,
  category_id INT64
);

-- Bảng 6: Location
CREATE TABLE Location (
  _ip STRING,
  country_name STRING,
  country_code STRING,
  latitude STRING,
  longitude STRING
);

-- Bảng 7: User
CREATE TABLE User (
  user_id INT64,
  device_id STRING,
  email_address STRING,
  event_id STRING
);

-- Bảng 8: Store
CREATE TABLE Store (
  store_id INT64,
  store_name STRING,
  event_id STRING
);

-- Bảng 9: Event
CREATE TABLE Event (
  _id STRING,
  time_stamp INT64,
  ip STRING,
  user_agent STRING,
  resolution STRING,
  api_version STRING,
  local_time DATETIME,
  show_recommendation BOOLEAN,
  current_url STRING,
  referrer_url STRING,
  collection STRING,
  cat_id INT64,
  is_paypal BOOLEAN,
  recommendation_product_id INT64,
  recommendation_product_position STRING,
  recommendation_clicked_position STRING,
  key_search STRING,
  order_id INT64,
  viewing_product_id INT64,
  recommendation BOOLEAN,
  utm_source STRING,
  utm_medium STRING,
  product_id INT64,
  collect_id STRING,
  price FLOAT64,
  currency STRING
);

-- Bảng 10: Event_Recommend
CREATE TABLE Event_Recommend (
  event_id STRING,
  recommend_option_id INT64
);

-- Bảng 11: Event_Option
CREATE TABLE Event_Option (
  event_id STRING,
  product_option_id INT64,
  product_quality_id INT64
);

-- Bảng 12: Event_Cart
CREATE TABLE Event_Cart (
  event_id STRING,
  product_id INT64,
  amount INT64,
  price FLOAT64,
  currency STRING
);

-- Bảng 13: Cart_Product_Option
CREATE TABLE Cart_Product_Option (
  product_option_id INT64,
  event_id STRING,
  product_id INT64
);

-- Bảng 14: Event_Filter
CREATE TABLE Event_Filter (
  event_id STRING,
  filter_option_id INT64
);
