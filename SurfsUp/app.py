from flask import Flask, jsonify
from sqlalchemy import func
import datetime as dt
import pandas as pd

app = Flask(__name__)

# Create a session for database interaction
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

# Set up the engine and reflect the tables
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

# Access the tables
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(engine)

# Homepage route
@app.route("/")
def home():
    return (
        f"Welcome to the Climate API!<br>"
        f"Available Routes:<br>"
        f"/api/v1.0/precipitation<br>"
        f"/api/v1.0/stations<br>"
        f"/api/v1.0/tobs<br>"
        f"/api/v1.0/&lt;start&gt;<br>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br>"
    )

# Precipitation route: Last 12 months of precipitation data
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Calculate the date one year from the most recent observation
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    last_year_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - pd.DateOffset(years=1)
    last_year_date_str = last_year_date.strftime('%Y-%m-%d')
    
    # Perform the query to retrieve the precipitation data for the last 12 months
    results = session.query(Measurement.date, Measurement.prcp).filter(
        Measurement.date >= last_year_date_str
    ).all()

    # Convert the query results to a dictionary
    precipitation_data = {date: prcp for date, prcp in results}
    
    return jsonify(precipitation_data)

# Stations route: List of stations
@app.route("/api/v1.0/stations")
def stations():
    # Perform the query to get all stations
    results = session.query(Station.station).all()

    # Convert the results to a list of stations
    stations_list = [station[0] for station in results]
    
    return jsonify(stations_list)

# TOBS route: Temperature observations for the most active station for the previous year
@app.route("/api/v1.0/tobs")
def tobs():
    # Assuming the most active station is the first result from the previous query
    most_active_station_id = session.query(Station.station).join(Measurement, Station.station == Measurement.station) \
        .group_by(Station.station).order_by(func.count(Measurement.station).desc()).first()[0]
    
    # Calculate the date one year from the most recent observation
    most_recent_date = session.query(func.max(Measurement.date)).scalar()
    last_year_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - pd.DateOffset(years=1)
    last_year_date_str = last_year_date.strftime('%Y-%m-%d')
    
    # Query the temperature data for the most active station for the last 12 months
    results = session.query(Measurement.date, Measurement.tobs).filter(
        Measurement.station == most_active_station_id,
        Measurement.date >= last_year_date_str
    ).all()

    # Convert the results to a list of temperature observations
    tobs_list = [{"date": date, "temperature": tobs} for date, tobs in results]
    
    return jsonify(tobs_list)

# Start and End date range route: Temperature stats (TMIN, TAVG, TMAX) for a given start or start-end date
@app.route("/api/v1.0/<start>")
def start_date(start):
    # Perform the query to calculate TMIN, TAVG, TMAX for all dates greater than or equal to start date
    results = session.query(
        func.min(Measurement.tobs).label('TMIN'),
        func.avg(Measurement.tobs).label('TAVG'),
        func.max(Measurement.tobs).label('TMAX')
    ).filter(Measurement.date >= start).all()

    # Return the results as a JSON response
    return jsonify(results[0])

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    # Perform the query to calculate TMIN, TAVG, TMAX for the date range start to end
    results = session.query(
        func.min(Measurement.tobs).label('TMIN'),
        func.avg(Measurement.tobs).label('TAVG'),
        func.max(Measurement.tobs).label('TMAX')
    ).filter(Measurement.date >= start, Measurement.date <= end).all()

    # Return the results as a JSON response
    return jsonify(results[0])

if __name__ == "__main__":
    app.run(debug=True)
