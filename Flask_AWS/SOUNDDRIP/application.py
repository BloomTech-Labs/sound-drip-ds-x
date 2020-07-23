from flask import (Flask, request, jsonify)
from flask_cors import CORS
from models.predict import Sound_Drip,Slider
import pickle
from joblib import load
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import MinMaxScaler
import numpy as np

genre_array = pickle.load(open("./data/genres_array_2.pkl", "rb"))
song_id_array = pickle.load(open("./data/song_id_array3.pkl", "rb"))
scaler = load("./models/scalar3.joblib")
model = load('./models/model5.joblib')
slider_model = load('./models/slider_model6.joblib')

# Create Flask app. Should use "application" as variable name for AWS EBS
application = Flask(__name__)
CORS(application)



# Main Default page
@application.route("/request", methods=['GET', 'POST'])
def prediction():
  ''''request flask route takes token passed in from FE POST and outputs the 20 most similar songs'''
  content = request.get_json(silent=True)
  token = content["token"]
  SdObj = Sound_Drip(token, genre_array, song_id_array, scaler, model)
  song_id_predictions = SdObj.song_id_predictions[0]
  return song_id_predictions,print('JSON OBJECT Returned')
  # return jsonify(song_id_predictions),print('JSON Object Returned')


# Slider endpoint
@application.route("/slider", methods=['GET', 'POST'])
def slider_prediction():
  ''''request flask route takes acoustical features object and outputs the 20 most similar songs'''
  content = request.get_json(silent=True)
  slider = Slider(content, scaler, slider_model, song_id_array)
  slider_predictions = slider.slider_predictions[0]
  return slider_predictions, print("JSON Slider Object Returned")
  # return jsonify(slider_predictions, print('Slider JSON Object Returned'))


@application.route("/")
def root():
    return """Hello, I am working right now. send your request to {/request}"""


if __name__ == "__main__":
    application.run(debug=True)
