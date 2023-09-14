import httpx  # requests capability, but can work with async
from prefect import flow, task


@task(log_prints=True)
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly=["temperature_2m", "windspeed_10m"]),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    most_recent_wind_speed = float(weather.json()["hourly"]["windspeed_10m"][0])
    return most_recent_temp, most_recent_wind_speed


@task(log_prints=True)
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"


@flow(log_prints=True)
def pipeline(lat=38.9, lon=-77.0):
    temp, wind = fetch_weather(lat, lon)
    temperature = save_weather(temp)
    wind = save_weather(wind)
    return temperature, wind



if __name__ == "__main__":
    pipeline(name='flow_sander')
