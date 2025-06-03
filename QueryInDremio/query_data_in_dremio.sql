--------- Batch Data ---------------------
-- Bản đồ phân bố đơn hàng
WITH order_counts AS (
    SELECT 
        c.customer_unique_id,
        COUNT(o.order_id) AS order_count
    FROM 
        "nhom6_lakehouse".gold.export."dim_order" AS o
    JOIN 
        "nhom6_lakehouse".gold.export."dim_customer" AS c
        ON o.customer_id = c.customer_id
    GROUP BY 
        c.customer_unique_id
),
unique_geolocation AS (
    SELECT 
        geolocation_zip_code_prefix,
        MIN(geolocation_lat) AS geolocation_lat,
        MIN(geolocation_lng) AS geolocation_lng
    FROM "nhom6_lakehouse".gold.export."dim_geolocation"
    GROUP BY geolocation_zip_code_prefix
)

SELECT 
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    g.geolocation_lat,
    g.geolocation_lng,
    oc.order_count
FROM 
    "nhom6_lakehouse".gold.export."dim_customer" AS c
JOIN 
    unique_geolocation AS g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
LEFT JOIN 
    order_counts AS oc
    ON c.customer_unique_id = oc.customer_unique_id;

-- Phân khúc khách hàng theo thành phố với CAST
WITH thong_ke_khach_hang AS (
    SELECT 
        c.customer_unique_id,
        c.customer_city as thanh_pho,
        c.customer_state as tinh_bang,
        COUNT(f.factOrderKey) as tong_so_don,
        SUM(CAST(COALESCE(s.totalOrderValue, '0') AS DECIMAL(10,2))) as tong_chi_tieu,
        AVG(CAST(COALESCE(s.totalOrderValue, '0') AS DECIMAL(10,2))) as gia_tri_trung_binh_don_hang
    FROM "nhom6_lakehouse".gold.export.FactOrders f
    JOIN "nhom6_lakehouse".gold.export."dim_customer" c ON f.customerKey = c.customerKey
    JOIN "nhom6_lakehouse".gold.export.FactSales s ON f.orderKey = s.orderKey
    GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
),
phan_khuc_theo_thanh_pho AS (
    SELECT 
        thanh_pho,
        tinh_bang,
        customer_unique_id,
        CASE 
            WHEN tong_chi_tieu >= 1000 THEN 'Khách VIP'
            WHEN tong_chi_tieu >= 500 THEN 'Khách giá trị cao'
            WHEN tong_chi_tieu >= 200 THEN 'Khách trung bình'
            ELSE 'Khách giá trị thấp'
        END as phan_khuc_khach_hang,
        tong_chi_tieu
    FROM thong_ke_khach_hang
    WHERE thanh_pho IS NOT NULL
)
SELECT 
    thanh_pho,
    tinh_bang,
    COUNT(*) as tong_so_khach,
    COUNT(CASE WHEN phan_khuc_khach_hang = 'Khách VIP' THEN 1 END) as so_khach_vip,
    COUNT(CASE WHEN phan_khuc_khach_hang = 'Khách giá trị cao' THEN 1 END) as so_khach_gia_tri_cao,
    COUNT(CASE WHEN phan_khuc_khach_hang = 'Khách trung bình' THEN 1 END) as so_khach_trung_binh,
    COUNT(CASE WHEN phan_khuc_khach_hang = 'Khách giá trị thấp' THEN 1 END) as so_khach_gia_tri_thap,
    ROUND(AVG(tong_chi_tieu), 2) as chi_tieu_trung_binh_thanh_pho,
    ROUND(COUNT(CASE WHEN phan_khuc_khach_hang = 'Khách VIP' THEN 1 END) * 100.0 / COUNT(*), 2) as ty_le_khach_vip
FROM phan_khuc_theo_thanh_pho
GROUP BY thanh_pho, tinh_bang
HAVING COUNT(*) >= 50  
ORDER BY chi_tieu_trung_binh_thanh_pho DESC, ty_le_khach_vip DESC;

-- Doanh thu sản phẩm
SELECT 
    p.productKey,
    p.product_id,
    p.product_category_name,
    COUNT(fs.salesKey) AS so_luong_ban,

    SUM(CAST(COALESCE(fs.totalOrderValue, '0') AS DECIMAL(10,2))) AS doanh_thu,
    AVG(CAST(COALESCE(fs.price, '0') AS DECIMAL(10,2))) AS gia_trung_binh

FROM "nhom6_lakehouse".gold.export.FactSales fs
JOIN "nhom6_lakehouse".gold.export.dim_product p 
    ON fs.productKey = p.productKey
WHERE p.product_category_name IS NOT NULL
GROUP BY 
    p.productKey,
    p.product_id,
    p.product_category_name
ORDER BY doanh_thu DESC;

-- FactOrders
SELECT 
    f.factOrderKey,
    f.customerKey,
    f.orderPaymentKey,
    f.orderDateKey,
    f.approvedDateKey,
    f.deliveryDateKey,
    f.estimatedDeliveryDateKey,
    
    -- Convert 2 cột này thành INTEGER
    CONVERT_TO_INTEGER(f.delivery_delay, 1, 1, 0) AS delivery_delay,
    CONVERT_TO_INTEGER(f.delivery_time, 1, 1, 0) AS delivery_time,
    
    f.order_item_count,
    f.order_processing_time,
    
    c.customer_unique_id,
    pay.payment_type,
    d1.full_date AS order_date,
    d2.full_date AS approved_date,
    d3.full_date AS delivery_date,
    d4.full_date AS estimated_delivery_date,
    
    DATEDIFF(CAST(d4.full_date AS DATE), CAST(d1.full_date AS DATE)) AS numberEstimatedDelivery,

    o.order_id,

    SUM(CAST(COALESCE(oi.price, '0') AS DECIMAL(10,2))) + 
    MAX(CAST(COALESCE(oi.freight_value, '0') AS DECIMAL(10,2))) AS total_value,
    MAX(CAST(COALESCE(oi.freight_value, '0') AS DECIMAL(10,2))) AS freight_value,
    COUNT(oi.order_item_id) AS item_count,
    MAX(p.product_category_name) AS sample_category,
    MAX(s.seller_city) AS sample_seller_city,
    MAX(s.seller_state) AS sample_seller_state
FROM 
    "nhom6_lakehouse".gold.export."FactOrders" AS f
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_customer" AS c
    ON f.customerKey = c.customerKey
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_order_payment" AS pay
    ON f.orderPaymentKey = pay.orderPaymentKey
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_date" AS d1
    ON f.orderDateKey = d1.date_key
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_date" AS d2
    ON f.approvedDateKey = d2.date_key
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_date" AS d3
    ON f.deliveryDateKey = d3.date_key
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_date" AS d4
    ON f.estimatedDeliveryDateKey = d4.date_key
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_order" AS o
    ON f.orderKey = o.orderKey
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_order_item" AS oi
    ON o.order_id = oi.order_id
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_product" AS p
    ON oi.product_id = p.product_id
LEFT JOIN 
    "nhom6_lakehouse".gold.export."dim_seller" AS s
    ON oi.seller_id = s.seller_id
GROUP BY 
    f.factOrderKey,
    f.customerKey,
    f.orderPaymentKey,
    f.orderDateKey,
    f.approvedDateKey,
    f.deliveryDateKey,
    f.estimatedDeliveryDateKey,
    f.delivery_delay,
    f.delivery_time,
    f.order_item_count,
    f.order_processing_time,
    c.customer_unique_id,
    pay.payment_type,
    d1.full_date,
    d2.full_date,
    d3.full_date,
    d4.full_date,
    o.order_id;

-- Các phương thức thanh toán
SELECT 
    pay.orderPaymentKey,
    pay.order_id,
    pay.payment_type AS hinh_thuc_thanh_toan,
    COUNT(*) AS so_giao_dich,
    AVG(CAST(COALESCE(pay.payment_value, '0') AS DECIMAL(10,2))) AS gia_tri_tb,
    SUM(CAST(COALESCE(pay.payment_value, '0') AS DECIMAL(10,2))) AS tong_gia_tri,
    AVG(CAST(COALESCE(pay.payment_installments, '0') AS INT)) AS tra_gop_tb
FROM "nhom6_lakehouse".gold.export.dim_order_payment pay
GROUP BY 
    pay.orderPaymentKey,
    pay.order_id,
    pay.payment_type
ORDER BY so_giao_dich DESC;

-- Market Share Categories theo thời gian
SELECT 
    d."year",
    d.quarter,
    p.product_category_name,
    SUM(CAST(COALESCE(fs.totalOrderValue, '0') AS DECIMAL(10,2))) as doanh_thu,
    COUNT(fs.salesKey) as so_luong_ban
FROM "nhom6_lakehouse".gold.export.FactSales fs
JOIN "nhom6_lakehouse".gold.export."dim_product" p ON fs.productKey = p.productKey
JOIN "nhom6_lakehouse".gold.export."dim_date" d ON fs.dateKey = d.date_key
WHERE p.product_category_name IS NOT NULL
GROUP BY d."year", d.quarter, p.product_category_name
ORDER BY d."year", d.quarter, doanh_thu DESC;


-- Tỉ lệ điểm đánh giá
SELECT 
    r.review_score as diem_danh_gia,
    COUNT(*) as so_luong_review,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ty_le_phan_tram
FROM "nhom6_lakehouse".gold.export.FactReview fr
JOIN "nhom6_lakehouse".gold.export."dim_order_review" r ON fr.reviewKey = r.reviewKey
WHERE r.review_score IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score;

-- Trải nghiệm của khách hàng
SELECT 
    CASE 
        WHEN review_score = '1' THEN '1★'
        WHEN review_score = '2' THEN '2★' 
        WHEN review_score = '3' THEN '3★'
        WHEN review_score = '4' THEN '4★'
        WHEN review_score = '5' THEN '5★'
    END AS diem_review,
    COUNT(DISTINCT customerKey) AS so_khach_hang,
    COUNT(DISTINCT customerKey) - LAG(COUNT(DISTINCT customerKey), 1, 0) OVER (ORDER BY review_score) AS thay_doi
FROM nhom6_lakehouse.gold.export.FactReview 
GROUP BY review_score
ORDER BY review_score;


--------- Streaming Data ---------------------
-- Sự phân bố nhiệt độ trung bình theo từng phút
SELECT
  t."minute"             AS minute_of_hour,
  AVG(fw.temp)           AS total_temp
FROM fact_weather fw
JOIN dim_time      t     ON fw.time_id = t.time_id
GROUP BY t."minute"
ORDER BY t."minute";

-- Độ che phủ mây và tốc độ gió trung bình của từng thành phố
SELECT
  c.city_name                                  AS city,
  ROUND(AVG(fw.cloud_coverage),1)              AS avg_cloud_pct,
  ROUND(AVG(fw.wind_speed),1)                  AS avg_wind_speed
FROM fact_weather fw
JOIN dim_city      c  ON fw.city_id = c.city_id
GROUP BY c.city_name
ORDER BY c.city_name;

-- Tỷ lệ phân bố điều kiện thời tiết 
WITH distinct_counts AS (
  SELECT
    wc.weather_main,
    COUNT(DISTINCT fw.condition_id) AS cnt
  FROM fact_weather fw
  JOIN dim_weather_condition wc
    ON fw.condition_id = wc.condition_id
  GROUP BY wc.weather_main
)
SELECT
  weather_main,
  cnt AS sum_of_each_weather_main,
  ROUND(100.0 * cnt / SUM(cnt) OVER (), 1) AS percent_of_weather_main
FROM distinct_counts
ORDER BY cnt DESC;

-- Nhiệt độ trung bình theo thành phố và điều kiện thời tiết
SELECT
  c.city_name,
  wc.weather_main,
  ROUND(AVG(fw.temp),1) AS avg_temp
FROM fact_weather fw
JOIN dim_city               c  ON fw.city_id        = c.city_id
JOIN dim_weather_condition wc ON fw.condition_id   = wc.condition_id
GROUP BY c.city_name, wc.weather_main
ORDER BY c.city_name, wc.weather_main;
