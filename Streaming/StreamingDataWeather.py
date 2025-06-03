import requests, json, time, sys
from datetime import datetime
import pytz
from confluent_kafka import Producer

# Sửa lỗi encoding nếu terminal không phải UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# Azure Event Hub config
KAFKA_TOPIC = 'myeventhub'  # phải trùng tên Event Hub đã tạo
conf = {
    'bootstrap.servers': 'ehnamespace-kafka.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://ehnamespace-kafka.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c2nNhCQl+5k70RZ2XhsvjDdh18g1kmtyM+AEhEh6/Dk=',
    'client.id': 'weather-producer'
}

# Danh sách thành phố Việt Nam
cities = [
    {"name": "HoChiMinh", "lat": 10.7769, "lon": 106.7009},
    {"name": "Hanoi", "lat": 21.0285, "lon": 105.8542},
    {"name": "Danang", "lat": 16.0471, "lon": 108.2068},
    {"name": "GiaLai", "lat": 13.9833, "lon": 108.0000},
    {"name": "CanTho", "lat": 10.0452, "lon": 105.7469},
    {"name": "Hue", "lat": 16.4637, "lon": 107.5909}
]

API_KEY = "your_openweathermap_api_key" 
vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')


def get_weather_data(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "appid": API_KEY,
        "units": "metric",
        "lang": "vi"
    }

    try:
        response = requests.get(url, params=params)
        data = response.json()
        data["city_name"] = city["name"]
        data["timestamp"] = datetime.now(vietnam_tz).isoformat()
        return data
    except Exception as e:
        print(f"[{datetime.now(vietnam_tz).isoformat()}] ❌ Error getting {city['name']}: {e}")
        return None


def stream_weather_to_eventhub(producer, topic, interval=300):
    print(f"🌤️ Bắt đầu streaming thời tiết vào Event Hub: {topic}")
    while True:
        batch = []
        for city in cities:
            data = get_weather_data(city)
            print(data)
            if data:
                msg = json.dumps(data)
                producer.produce(topic, value=msg)
                batch.append(city["name"])
                print(f"✅ Sent: {city['name']}")
                time.sleep(1)

        producer.flush()
        print(f"Sent {len(batch)} cities at {datetime.now(vietnam_tz).strftime('%H:%M:%S')}")
        time.sleep(interval)


# Entry point
if __name__ == "__main__":
    producer = Producer(**conf)
    try:
        stream_weather_to_eventhub(producer, KAFKA_TOPIC, interval=30)  # 30 giây
    except KeyboardInterrupt:
        print("Dừng streaming theo yêu cầu người dùng.")