require 'sinatra'
require 'csv'
require_relative './models/sensor.rb'
require_relative './models/sensor_data.rb'

configure do
  Mongoid.load!('./mongoid.yml')
end

NO_DATA_RESPONSE = "No data available."
NULL_VALUE_REPLACEMENT = 66666

def replace_null_value(value)
    value.nil? ? NULL_VALUE_REPLACEMENT : value
end

def prepare_for_export(sensor_data)
    converted_data = sensor_data.asc(:date).map do |e|
        {
            sensor_id: e.sensor.nil? ? :null : e.sensor.sensor_id,
            date: e.date,
            partikelPM10Mittel: replace_null_value(e.partikelPM10Mittel),
            # partikelPM10Ueberschreitungen: replace_null_value(e.partikelPM10Ueberschreitungen),
            russMittel: replace_null_value(e.russMittel),
            russMax3h: replace_null_value(e.russMax3h),
            stickstoffdioxidMittel: replace_null_value(e.stickstoffdioxidMittel),
            stickstoffdioxidMax1h: replace_null_value(e.stickstoffdioxidMax1h),
            benzolMittel: replace_null_value(e.benzolMittel),
            benzolMax1h: replace_null_value(e.benzolMax1h),
            kohlenmonoxidMittel: replace_null_value(e.kohlenmonoxidMittel),
            kohlenmonoxidMax8hMittel: replace_null_value(e.kohlenmonoxidMax8hMittel),
            ozonMax1h: replace_null_value(e.ozonMax1h),
            ozonMax8hMittel: replace_null_value(e.ozonMax8hMittel),
            schwefeldioxidMittel: replace_null_value(e.schwefeldioxidMittel),
            schwefeldioxidMax1h: replace_null_value(e.schwefeldioxidMax1h)
        }
    end
    converted_data
end

def convert_to_json(sensor_data)
    data = prepare_for_export(sensor_data)
    data = NO_DATA_RESPONSE if data.nil? || data.empty?
    data.to_json
end

def convert_to_csv(sensor_data)
    data = prepare_for_export sensor_data
    return NO_DATA_RESPONSE if data.nil? || data.empty?
    csv_string = CSV.generate do |csv|
        csv << data.first.keys
        data.each do |hash|
            csv << hash.values
        end
    end
    csv_string
end

def sensor_data_for_station(station)
  sensor = Sensor.for_sensor(station)
  sensor.sensor_data
end

def sensor_data_for_station_by_year(station, year)
  sensor_data_for_station(station).for_year(year)
end

get '/api/v1/stations' do
  content_type :json
  Sensor.all.map { |e| {sensor_id: e.sensor_id} }.to_json
end

get '/api/v1/stations/:station' do
  content_type :json
  convert_to_json sensor_data_for_station(params[:station])
end

get '/api/v1/stations/:station/csv' do
  content_type :csv
  convert_to_csv sensor_data_for_station(params[:station])
end

get '/api/v1/stations/:station/sensordata/:year' do
  content_type :json
  convert_to_json sensor_data_for_station_by_year(params[:station], params[:year].to_i)
end

get '/api/v1/stations/:station/sensordata/:year/csv' do
  content_type :csv
  convert_to_csv sensor_data_for_station_by_year(params[:station], params[:year].to_i)
end

get '/api/v1/sensordata/:year' do
  content_type :json
  convert_to_json SensorData.for_year(params[:year].to_i)
end

get '/api/v1/sensordata/:year/csv' do
  content_type :csv
  convert_to_csv SensorData.for_year(params[:year].to_i)
end

get '/api/v1/recent' do
  content_type :json
  convert_to_json SensorData.recent
end

get '/api/v1/recent/csv' do
  content_type :csv
  convert_to_csv SensorData.recent
end

