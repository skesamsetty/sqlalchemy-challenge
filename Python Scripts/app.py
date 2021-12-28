from flask import Flask, jsonify

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from datetime import date, datetime as dt

# create engine to hawaii.sqlite
# database_path = "/Resources/hawaii.sqlite"
# engine = create_engine(f"sqlite:///{database_path}")

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base=automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement=Base.classes.measurement
Station=Base.classes.station


# Setup Flask app
app = Flask(__name__)

# Route definitions for the webpage

#  Home page or Landing page
@app.route('/')
def index():
    # Returns list of all the routes that can be navigated from home page.
        return (
                f"Welcome! <br> <b> Available endpoints</b> <br>"
                f"/api/v1.0/precipitation<br>"
                f"/api/v1.0/stations<br>"
                f"/api/v1.0/tobs<br>"
                f"/api/v1.0/< start >  <i>(Date format should be yyyy-mm-dd)</i><br>"
                f"/api/v1.0/< start > / < end > <i>(Date format should be yyyy-mm-dd)</i><br>"
                )


# Precipitation page
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Returns JSON results for the precipitation page
    session = Session(engine)

    resultSet = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= previous_year_date).all()

    session.close()

    precipitationDF = {}
    for date, precipitation in resultSet:
        precipitationDF[date] = precipitation
    
    return jsonify(precipitationDF)


# Stations page
@app.route("/api/v1.0/stations")
def stations():
    # Returns JSON results for the stations page
    session = Session(engine)

    resultSet = session.query(Station.id, Station.name).all()

    session.close()

    stationDF = {}
    for id, station in resultSet:
        stationDF[id] = station

    print(stationDF)
      
    return jsonify(stationDF)


# Temperature Observations page
@app.route("/api/v1.0/tobs")
def tobs():
    # Returns JSON results for the temperature observations page
    session = Session(engine)

    # Fetch Active Station
    stationSummary = session.query(Measurement.station, Station.name, func.count(Measurement.id).label('count'))\
                                    .join(Station, Measurement.station == Station.station)\
                                    .group_by(Measurement.station)\
                                    .order_by(desc('count'))\
                                    .all()

    activeStationID = stationSummary[0][0]
    activeStationName = stationSummary[0][1]

    #  Fetch Temperature observations for the most active station
    resultSet = session.query(Measurement.date, Measurement.tobs) \
                            .filter(Measurement.station == activeStationID)\
                            .filter(Measurement.date >= previous_year_date)\
                            .all()

    session.close()
  
    # Add Active station to the dictionary. 
    # Added a space in the front of the key --> so it appears before the date key
    tobsDF = {" Active Station ID": activeStationID, " Active Station Name": activeStationName}

    for date, tobs in resultSet:
        tobsDF[date] = tobs

    return jsonify(tobsDF)


# Summary temperature observations by Start Date page
@app.route("/api/v1.0/<start>")
def SummaryByStartDate(start):
    # Returns JSON results for the Summary page by start date to show Minimum, Maximum and Average temperature observations
    session = Session(engine)

    endDate = recent_date
    startDate = start

    resultSet = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))\
                                    .filter(Measurement.date >= startDate)\
                                    .first()

    session.close()

    return (
        f"==============================================<br>"
        f"SUMMARY - Start date: {startDate} & End date: {endDate}<br>"
        f"==============================================<br>"
        f"Minimum temperature: {resultSet[0]:.2f}<br>"
        f"Maximum temperature: {resultSet[1]:.2f}<br>"
        f"Average temperature: {resultSet[2]:.2f}<br>"
        f"==============================================<br>"
        )

# Summary temperature observations by Start and End Dates page
@app.route("/api/v1.0/<start>/<end>")
def SummaryByStartEndDate(start, end):
    # Returns JSON results for the Summary page by start date and end date to show Minimum, Maximum and Average temperature observations
    session = Session(engine)

    endDate = end
    startDate = start

    resultSet = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))\
                                    .filter(Measurement.date >= startDate)\
                                    .filter(Measurement.date >= endDate)\
                                    .first()

    session.close()

    return (
        f"==============================================<br>"
        f"SUMMARY - Start date: {startDate} & End date: {endDate}<br>"
        f"==============================================<br>"
        f"Minimum temperature: {resultSet[0]:.2f}<br>"
        f"Maximum temperature: {resultSet[1]:.2f}<br>"
        f"Average temperature: {resultSet[2]:.2f}<br>"
        f"==============================================<br>"
        )


# Common function to fetch the most recent date and corresponding previous one year date
def loadDatasetDates():
    session = Session(engine)

    global recent_date
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    print(f"Most recent date in Dataset: {recent_date}")

    global previous_year_date
    previous_year_date = dt.strptime(recent_date, "%Y-%m-%d") + relativedelta(years=-1)
    print(f"Previous year date for most recent date: {previous_year_date}")

    session.close()

    # return (recent_date, previous_year_date)


if __name__ == "__main__":
    loadDatasetDates()
    app.run(debug=True)